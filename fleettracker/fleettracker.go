package fleettracker

import (
	"fmt"
	"sync"
	"time"

	dockerapi "github.com/fsouza/go-dockerclient"
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/dockerutil"
	"github.com/leveros/leveros/hostman"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/scale"
	"golang.org/x/net/context"
)

// PackageName is the name of this package.
const PackageName = "fleettracker"

var logger = leverutil.GetLogger(PackageName, "fleettracker")

// FleetTrackerService is the name of the fleettracker internal service.
const FleetTrackerService = "fleettracker"

var (
	// InstanceIDFlag is the instance ID of the fleettracker. Note: This is a
	// different instance ID than leverutil.InstanceIDFlag because they serve
	// different things.
	InstanceIDFlag = config.DeclareString(
		PackageName, "instanceID", leverutil.RandomID())
	// TTLFlag is the time after a service expires in fleettracker. If no RPC
	// event comes up during this span of time for a Lever service, fleettracker
	// stops keeping track of its stats (it is assumed all its instances
	// have expired anyway).
	TTLFlag = config.DeclareDuration(PackageName, "ttl", 20*time.Minute)
)

// FleetTracker ensures that each Lever service has the right number of
// instances in its fleet. It spins up or kills instances as necessary.
type FleetTracker struct {
	grpcPool   *scale.GRPCPool
	docker     *dockerapi.Client
	serviceSKA *scale.SelfKeepAlive

	// Guard the following.
	lock sync.RWMutex
	// servingID -> servingTracker
	services map[string]*LoadTracker
}

// NewFleetTracker returns a new FleetTracker.
func NewFleetTracker(
	grpcServer *grpc.Server, grpcPool *scale.GRPCPool, docker *dockerapi.Client,
	grpcAddr string) (tracker *FleetTracker, err error) {
	tracker = &FleetTracker{
		grpcPool: grpcPool,
		docker:   docker,
		services: make(map[string]*LoadTracker),
	}
	RegisterFleetTrackerServer(grpcServer, tracker)
	instanceID := InstanceIDFlag.Get()
	serviceTTL := 30 * time.Second
	err = scale.RegisterServiceLocal(
		FleetTrackerService, instanceID, grpcAddr, serviceTTL)
	if err != nil {
		return nil, err
	}
	tracker.serviceSKA = scale.NewServiceSelfKeepAlive(instanceID, serviceTTL/2)
	return tracker, nil
}

// HandleOnRPC implements the FleetTrackerServer interface.
func (tracker *FleetTracker) HandleOnRPC(
	ctx context.Context, rpcEvent *RPCEvent) (*HandleReply, error) {
	err := tracker.OnRPC(rpcEvent)
	if err != nil {
		return nil, err
	}
	return &HandleReply{}, nil
}

// OnRPC should be called on each RPC that we want to track.
func (tracker *FleetTracker) OnRPC(rpcEvent *RPCEvent) error {
	tracker.lock.RLock()
	servingTracker, ok := tracker.services[rpcEvent.ServingID]
	tracker.lock.RUnlock()
	if !ok {
		// Handling new service.
		tracker.lock.Lock()
		// Make sure nothing changed when we switched to writing.
		_, ok := tracker.services[rpcEvent.ServingID]
		if ok {
			tracker.lock.Unlock()
			// Something changed while we switched locks. Try again.
			return tracker.OnRPC(rpcEvent)
		}

		codeDir := dockerutil.CodeDirPath(
			rpcEvent.Environment, rpcEvent.Service, rpcEvent.CodeVersion)
		leverConfig, err := core.ReadLeverConfig(codeDir)
		if err != nil {
			tracker.lock.Unlock()
			return err
		}
		servingTracker = NewLoadTracker(
			rpcEvent.ServingID, leverConfig.MaxInstanceLoad,
			leverConfig.MinInstances, rpcEvent.SessionID)
		tracker.services[rpcEvent.ServingID] = servingTracker
		go tracker.monitorServiceResource(rpcEvent.SessionID)
		tracker.lock.Unlock()
	}

	delta := servingTracker.OnRPC(rpcEvent.RpcNanos)
	if delta == 0 {
		return nil
	} else if delta > 0 {
		return tracker.scaleUp(delta, rpcEvent)
	} else {
		return tracker.scaleDown(-delta, rpcEvent)
	}
}

// Close stops the fleettracker instance.
func (tracker *FleetTracker) Close() {
	tracker.serviceSKA.Stop()
	err := scale.DeregisterService(InstanceIDFlag.Get())
	if err != nil {
		logger.WithFields("err", err).Error(
			"Error deregistering fleettracker service")
	}
}

func (tracker *FleetTracker) scaleUp(delta int, rpcEvent *RPCEvent) error {
	logger.WithFields(
		"leverEnv", rpcEvent.Environment,
		"leverService", rpcEvent.Service,
		"codeVersion", rpcEvent.CodeVersion,
		"servingID", rpcEvent.ServingID,
		"deltaInstances", delta,
	).Info("Scaling up")

	// Read the entry point from the config.
	codeDir := dockerutil.CodeDirPath(
		rpcEvent.Environment, rpcEvent.Service, rpcEvent.CodeVersion)
	leverConfig, err := core.ReadLeverConfig(codeDir)
	if err != nil {
		return err
	}

	// Spin up.
	hadErrors := false
	for i := 0; i < delta; i++ {
		instanceID := leverutil.RandomID()
		containerID, node, err := dockerutil.StartDockerContainer(
			tracker.docker, rpcEvent.Environment, rpcEvent.Service,
			instanceID, rpcEvent.CodeVersion, rpcEvent.IsAdmin, leverConfig)
		if err != nil {
			logger.WithFields(
				"err", err,
				"leverEnv", rpcEvent.Environment,
				"leverService", rpcEvent.Service,
				"codeVersion", rpcEvent.CodeVersion,
				"servingID", rpcEvent.ServingID,
			).Error("Error starting docker container")
			hadErrors = true
			continue
		}

		err = hostman.InitializeInstance(
			tracker.grpcPool,
			&hostman.InstanceInfo{
				Environment:       rpcEvent.Environment,
				Service:           rpcEvent.Service,
				InstanceID:        instanceID,
				ContainerID:       containerID,
				ServingID:         rpcEvent.ServingID,
				LevInstResourceID: "",
				LevInstSessionID:  "",
			}, node)
		if err != nil {
			logger.WithFields(
				"err", err,
				"leverEnv", rpcEvent.Environment,
				"leverService", rpcEvent.Service,
				"codeVersion", rpcEvent.CodeVersion,
				"servingID", rpcEvent.ServingID,
				"node", node,
				"leverInstanceID", instanceID,
			).Error("Failed to initialize instance remotely")
			hadErrors = true
			continue
		}
	}
	if hadErrors {
		return fmt.Errorf("There were errors during scale down")
	}
	return nil
}

func (tracker *FleetTracker) scaleDown(delta int, rpcEvent *RPCEvent) error {
	logger.WithFields(
		"leverEnv", rpcEvent.Environment,
		"leverService", rpcEvent.Service,
		"codeVersion", rpcEvent.CodeVersion,
		"servingID", rpcEvent.ServingID,
		"deltaInstances", delta,
	).Info("Scaling down")

	consulHealth := config.GetConsulClient().Health()
	entries, _, err := consulHealth.Service(
		rpcEvent.ServingID, "", true, &consulapi.QueryOptions{
			RequireConsistent: true,
		})
	if err != nil {
		logger.WithFields(
			"err", err,
			"servingID", rpcEvent.ServingID,
		).Error("Error trying to ask Consul for instances")
	}
	if len(entries) < delta {
		delta = len(entries)
	}

	tmpRand := leverutil.GetRand()
	permutation := tmpRand.Perm(len(entries))
	leverutil.PutRand(tmpRand)
	shuffled := make([]*consulapi.ServiceEntry, len(entries))
	for from, to := range permutation {
		shuffled[to] = entries[from]
	}
	toRemove := shuffled[:delta]

	hadErrors := false
	for _, entry := range toRemove {
		err = hostman.StopInstance(
			tracker.grpcPool,
			&hostman.InstanceKey{
				Environment: rpcEvent.Environment,
				Service:     rpcEvent.Service,
				InstanceID:  entry.Service.ID,
				ServingID:   rpcEvent.ServingID,
			}, entry.Node.Node)
		if err != nil {
			logger.WithFields("err", err).Error(
				"Error trying to stop instance remotely")
			hadErrors = true
		}
	}
	if hadErrors {
		return fmt.Errorf("There were errors during scale down")
	}
	return nil
}

func (tracker *FleetTracker) monitorServiceResource(servingID string) {
	err := scale.WaitResource(FleetTrackerService, servingID)
	if err != nil {
		logger.WithFields("err", err).Error(
			"Error monitoring fleettracker resource")
	}
	// Expired.
	tracker.lock.Lock()
	defer tracker.lock.Unlock()
	loadTracker, ok := tracker.services[servingID]
	if !ok {
		return
	}
	loadTracker.Close()
	delete(tracker.services, servingID)
}
