package hostman

import (
	"fmt"
	"sync"
	"time"

	dockerapi "github.com/fsouza/go-dockerclient"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/scale"
	"golang.org/x/net/context"
	grpc "github.com/leveros/grpc-go"
)

// ManagerService is the name of the host manager internal service.
const ManagerService = "leverhostmanager"

var (
	// InstanceIDFlag is the instance ID of the manager. Note: This is
	// a different instance ID than leverutil.InstanceIDFlag because they
	// serve different things.
	InstanceIDFlag = config.DeclareString(
		PackageName, "managerInstanceID", leverutil.RandomID())

	// RegionalNetworkFlag is the name of the Docker network that the proxy
	// uses internally.
	RegionalNetworkFlag = config.DeclareString(
		PackageName, "regionalNetwork", "leveros_default")
)

// KeepAliveFun is a function that should be called on every event on a stream
// to signal the manager that the unerlying infrastructure is still being used.
type KeepAliveFun func(resourceName, levResResourceID, levResSessionID string)

type envEntry struct {
	// Guard the following, as well as network creation / destruction.
	envLock        sync.Mutex
	networkID      string
	networkIP      string
	ownIP          string
	leverInstances map[string]*LeverInstance // instanceID -> instance
}

// Manager manages the instances and resources available on this host.
type Manager struct {
	grpcPool    *scale.GRPCPool
	docker      *dockerapi.Client
	logger      *leverutil.Logger
	serviceSKA  *scale.SelfKeepAlive
	sessionSKA  *scale.SelfKeepAlive
	resource    *scale.Resource
	proxyInAddr string

	// Guard the following.
	lock         sync.Mutex
	environments map[string]*envEntry

	// Guard the following.
	servingIDsLock sync.RWMutex
	// servingID -> set of instanceIDs
	servingIDs map[string]map[string]struct{}
}

// NewManager returns a new Manager.
func NewManager(
	grpcServer *grpc.Server, grpcPool *scale.GRPCPool,
	docker *dockerapi.Client, grpcAddr string, proxyInAddr string) (
	manager *Manager, err error) {
	manager = &Manager{
		grpcPool:     grpcPool,
		docker:       docker,
		proxyInAddr:  proxyInAddr,
		environments: make(map[string]*envEntry),
		servingIDs:   make(map[string]map[string]struct{}),
		logger:       leverutil.GetLogger(PackageName, "Manager"),
	}
	RegisterManagerServer(grpcServer, manager)
	managerInstanceID := InstanceIDFlag.Get()
	serviceTTL := 30 * time.Second
	err = scale.RegisterServiceLocal(
		ManagerService, managerInstanceID, grpcAddr, serviceTTL)
	if err != nil {
		return nil, err
	}
	manager.serviceSKA = scale.NewServiceSelfKeepAlive(
		managerInstanceID, serviceTTL/2)
	err = manager.registerManagerResource(grpcAddr)
	if err != nil {
		return nil, err
	}
	return manager, nil
}

// InitializeInstance implements the ManagerService interface.
func (manager *Manager) InitializeInstance(
	ctx context.Context, info *InstanceInfo) (
	reply *InitReply, err error) {
	_, _, _, _, err = manager.EnsureInfrastructureInitialized(info)
	if err != nil {
		return nil, err
	}
	return &InitReply{}, nil
}

// StopInstance implements the ManagerService interface.
func (manager *Manager) StopInstance(
	ctx context.Context, instanceKey *InstanceKey) (
	reply *StopReply, err error) {
	manager.lock.Lock()
	entry, ok := manager.environments[instanceKey.Environment]
	manager.lock.Unlock()
	if !ok {
		return nil, fmt.Errorf("Environment not found on this host")
	}
	entry.envLock.Lock()
	instance, ok := entry.leverInstances[instanceKey.InstanceID]
	entry.envLock.Unlock()
	if !ok {
		return nil, fmt.Errorf("Instance not found")
	}
	instance.Close(false)
	return &StopReply{}, nil
}

// RandomInstaceID returns a random instanceID that serves provided servingID.
func (manager *Manager) RandomInstaceID(
	servingID string) (instanceID string, err error) {
	manager.servingIDsLock.RLock()
	defer manager.servingIDsLock.RUnlock()
	instances, ok := manager.servingIDs[servingID]
	if !ok {
		return "", fmt.Errorf("Serving ID not found")
	}
	tmpRand := leverutil.GetRand()
	instanceIndex := tmpRand.Intn(len(instances))
	leverutil.PutRand(tmpRand)
	index := 0
	for instanceID := range instances {
		if index == instanceIndex {
			return instanceID, nil
		}
		index++
	}
	panic(fmt.Errorf("Should never happen"))
}

// EnsureInfrastructureInitialized verifies that all the necessary
// infrastructure is registered, managed and ready to be used.
func (manager *Manager) EnsureInfrastructureInitialized(
	info *InstanceInfo) (
	envNetworkIP string, ownIP string, instanceAddr string,
	keepAlive KeepAliveFun, err error) {
	entry, err := manager.getEnvironment(info.Environment)
	if err != nil {
		return "", "", "", nil, err
	}
	defer entry.envLock.Unlock()

	instance, ok := entry.leverInstances[info.InstanceID]
	if ok {
		// Instance already managed.
		manager.logger.WithFields("leverInstanceID", info.InstanceID).Debug(
			"Instance already managed")
		keepAlive = func(
			resourceName, levResResourceID, levResSessionID string) {
			instance.KeepAlive(resourceName, levResResourceID, levResSessionID)
		}
		return entry.networkIP, entry.ownIP, instance.InstanceAddr(), keepAlive,
			nil
	}
	// Instance is new.
	manager.logger.WithFields("leverInstanceID", info.InstanceID).Debug(
		"Instance is new")
	if info.ContainerID == "" {
		// We don't have the container ID. Cannot initialize instance.
		return "", "", "", nil, fmt.Errorf("Need container ID")
	}

	// Connect the container to the env network bridge so we can talk to it
	// (forward RPCs).
	instanceIPv4, err := leverutil.ConnectToDockerEnvNetwork(
		manager.docker, info.ContainerID, entry.networkID)
	if err != nil {
		manager.logger.WithFields(
			"err", err,
			"leverEnv", info.Environment,
			"containerID", info.ContainerID,
		).Error("Error connecting instance to env network")
		removeErr := leverutil.RemoveDockerContainer(
			manager.docker, info.ContainerID)
		if removeErr != nil {
			manager.logger.WithFields(
				"containerID", info.ContainerID,
				"err", removeErr,
			).Error("Error trying to remove container after previous error")
		}
		return "", "", "", nil, err
	}
	instanceAddr = instanceIPv4 + ":" + core.InstanceListenPortFlag.Get()

	if info.Environment == "admin"+core.InternalEnvironmentSuffixFlag.Get() &&
		info.Service == "admin" {
		// Admin environment. Also connect it to the regional network.
		_, err := leverutil.ConnectToDockerEnvNetwork(
			manager.docker, info.ContainerID, RegionalNetworkFlag.Get())
		if err != nil {
			manager.logger.WithFields(
				"err", err,
				"leverEnv", info.Environment,
				"containerID", info.ContainerID,
			).Error("Error connecting admin instance to regional network")
			removeErr := leverutil.RemoveDockerContainer(
				manager.docker, info.ContainerID)
			if removeErr != nil {
				manager.logger.WithFields(
					"containerID", info.ContainerID,
					"err", removeErr,
				).Error(
					"Error trying to remove container after previous error")
			}
			return "", "", "", nil, err
		}
	}

	// Add the instance ID to the local serving ID map.
	manager.servingIDsLock.Lock()
	_, ok = manager.servingIDs[info.ServingID]
	if !ok {
		manager.servingIDs[info.ServingID] = make(map[string]struct{})
	}
	manager.servingIDs[info.ServingID][info.InstanceID] = struct{}{}
	manager.servingIDsLock.Unlock()

	// Start managing instance.
	instance = NewLeverInstance(
		info, instanceAddr, manager.proxyInAddr, manager.grpcPool,
		manager.docker,
		func(instanceID string, err error) {
			manager.logger.WithFields("leverInstanceID", instanceID).Debug(
				"Instance closed")
			manager.onInstanceClose(
				entry, info.Environment, instanceID, info.ServingID, err)
		})
	entry.leverInstances[info.InstanceID] = instance

	keepAlive = func(resourceName, levResResourceID, levResSessionID string) {
		instance.KeepAlive(resourceName, levResResourceID, levResSessionID)
	}
	return entry.networkIP, entry.ownIP, instanceAddr, keepAlive, nil
}

// Close stops the manager.
func (manager *Manager) Close() {
	manager.serviceSKA.Stop()
	err := scale.DeregisterService(InstanceIDFlag.Get())
	if err != nil {
		manager.logger.WithFields("err", err).Error(
			"Error deregistering manager service")
	}
	manager.sessionSKA.Stop()
	err = manager.resource.Deregister()
	if err != nil {
		manager.logger.WithFields("err", err).Error(
			"Error deregistering manager resource")
	}

	// TODO: Remove all instances and environments.
}

func (manager *Manager) registerManagerResource(grpcAddr string) error {
	node, err := scale.GetOwnNodeName()
	if err != nil {
		return err
	}
	ttl := 30 * time.Second
	resource := "swarm://" + node
	res, success, err := scale.RegisterResourceCustom(
		ManagerService, resource, grpcAddr, node, ttl)
	if err != nil {
		return err
	}
	if !success {
		// Leftover from a previous life. Clean up and try again.
		err := scale.ExistingResource(ManagerService, resource, "").Deregister()
		if err != nil {
			return err
		}
		return manager.registerManagerResource(grpcAddr)
	}
	manager.resource = res
	manager.sessionSKA = scale.NewSessionSelfKeepAlive(
		res.GetSessionID(), ttl/2)
	return nil
}

func (manager *Manager) getEnvironment(
	env string) (entry *envEntry, err error) {
	manager.lock.Lock()
	entry, exists := manager.environments[env]
	if exists {
		manager.lock.Unlock()
		entry.envLock.Lock()
		manager.logger.WithFields("leverEnv", env).Debug(
			"Reusing environment")
		return entry, nil
	}
	entry = &envEntry{
		leverInstances: make(map[string]*LeverInstance),
	}
	manager.environments[env] = entry
	// Lock env before manager unlock to make sure we're the first ones to lock
	// this entry.
	entry.envLock.Lock()
	manager.lock.Unlock()
	manager.logger.WithFields("leverEnv", env).Debug(
		"Adding environment")

	// Create network for the new env.
	networkID, networkIP, ownIP, err := leverutil.CreateDockerEnvNetwork(
		manager.docker, env)
	if err != nil {
		manager.logger.WithFields("err", err, "leverEnv", env).Error(
			"Error trying to create env network")
		manager.lock.Lock()
		delete(manager.environments, env)
		manager.lock.Unlock()
		entry.envLock.Unlock()
		return nil, err
	}
	entry.networkID = networkID
	entry.networkIP = networkIP
	entry.ownIP = ownIP

	return entry, nil
}

// TODO: This is not yet used.
func (manager *Manager) removeEnvironment(env string) (firstErr error) {
	manager.lock.Lock()
	entry, exists := manager.environments[env]
	if !exists {
		manager.logger.WithFields("leverEnv", env).Error(
			"Error trying to remove non-existent env")
		return fmt.Errorf("Environment not found")
	}
	manager.lock.Unlock()
	manager.logger.WithFields("leverEnv", env).Debug(
		"Removing environment")

	entry.envLock.Lock()
	for _, instance := range entry.leverInstances {
		instance.Close(true)
	}

	err := leverutil.RemoveDockerEnvNetwork(manager.docker, entry.networkID)
	if err != nil {
		manager.logger.WithFields("err", err, "leverEnv", env).Error(
			"Error trying to remove env network")
		if firstErr == nil {
			firstErr = err
		}
	}
	entry.envLock.Unlock()

	manager.lock.Lock()
	delete(manager.environments, env)
	manager.lock.Unlock()
	return
}

func (manager *Manager) onInstanceClose(
	entry *envEntry, env string, instanceID string, servingID string,
	err error) {
	// Remove instanceID from pool of the servingID.
	manager.servingIDsLock.Lock()
	delete(manager.servingIDs[servingID], instanceID)
	if len(manager.servingIDs[servingID]) == 0 {
		delete(manager.servingIDs, servingID)
	}
	manager.servingIDsLock.Unlock()

	// Remove instance entry.
	entry.envLock.Lock()
	delete(entry.leverInstances, instanceID)
	// TODO: Remove env if env now empty.
	entry.envLock.Unlock()

	if err != nil {
		manager.logger.WithFields(
			"err", err,
			"leverEnv", env,
			"leverInstanceID", instanceID,
		).Fatal("Instance closed with error")
	}
}
