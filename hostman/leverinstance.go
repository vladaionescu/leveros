package hostman

import (
	"fmt"
	"sync"
	"time"

	dockerapi "github.com/fsouza/go-dockerclient"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/dockerutil"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/scale"
)

var (
	// InstanceExpiryTimeFlag is the expiry time after its last use after which
	// a instance instance is evicted from the host.
	InstanceExpiryTimeFlag = config.DeclareDuration(
		PackageName, "instanceExpiryTime", 5*time.Minute)
)

// OnCloseFun is a function that is called when an instance or a resource is
// closed.
type OnCloseFun func(identifier string, err error)

// LeverInstance represents a Lever instance running on a Lever
// host. It takes care of monitoring the Docker container and the Consul lock
// associated with the instance and manages the instance's lifecycle. It also
// keeps track of the resources hosted on it and sends them keep alive's where
// appropriate.
type LeverInstance struct {
	leverEnv        string
	leverService    string
	instanceID      string
	containerID     string
	servingID       string
	levInstResource *scale.Resource
	instanceAddr    string
	conns           *scale.GRPCPool
	docker          *dockerapi.Client
	sessionKAB      *scale.KeepAliveBuffer
	serviceSKA      *scale.SelfKeepAlive
	expiryTimer     *time.Timer
	expiryDur       time.Duration
	onCloseFun      OnCloseFun
	logger          *leverutil.Logger

	// Guard the following.
	lock           sync.Mutex
	closing        bool
	leverResources map[string]*LeverResource
}

// NewLeverInstance returns a new instance of LeverInstance.
func NewLeverInstance(
	info *InstanceInfo, instanceAddr string, proxyInAddr string,
	conns *scale.GRPCPool, docker *dockerapi.Client,
	onCloseFun OnCloseFun) *LeverInstance {
	expiryDur := InstanceExpiryTimeFlag.Get()
	logger := leverutil.GetLogger(PackageName, "leverinstance").WithFields(
		"leverEnv", info.Environment,
		"leverService", info.Service,
		"leverInstanceID", info.InstanceID,
	)
	instance := &LeverInstance{
		leverEnv:     info.Environment,
		instanceID:   info.InstanceID,
		leverService: info.Service,
		containerID:  info.ContainerID,
		servingID:    info.ServingID,
		instanceAddr: instanceAddr,
		conns:        conns,
		docker:       docker,
		expiryDur:    expiryDur,
		onCloseFun:   onCloseFun,
		logger:       logger,
	}
	instance.expiryTimer = time.AfterFunc(expiryDur, instance.onExpired)
	instance.registerAsService(proxyInAddr)
	go instance.monitorContainer()
	if info.LevInstResourceID != "" {
		// This instance has a resource + session associated with it. Manage
		// its lifecycle.
		instance.levInstResource = scale.ExistingResource(
			leverutil.ServiceFlag.Get(), info.LevInstResourceID,
			info.LevInstSessionID)
		instance.sessionKAB = scale.NewSessionKeepAliveBuffer(
			info.LevInstSessionID, expiryDur)
		go instance.monitorInstanceLock(info.LevInstResourceID)
	}
	return instance
}

// KeepAlive resets the instance's TTL expiry along with any resource being
// accessed.
func (instance *LeverInstance) KeepAlive(
	leverResource, leverResResourceID, leverResSessionID string) error {
	instance.lock.Lock()
	defer instance.lock.Unlock()
	if instance.closing {
		return fmt.Errorf("Service closing")
	}

	instance.expiryTimer.Reset(instance.expiryDur)
	if instance.sessionKAB != nil {
		instance.sessionKAB.KeepAlive()
	}

	if leverResource != "" {
		resource, ok := instance.leverResources[leverResource]
		if !ok {
			var err error
			resource, err = NewLeverResource(
				instance.leverEnv, instance.leverService, instance.instanceID,
				leverResource, leverResResourceID, leverResSessionID,
				instance.instanceAddr, instance.conns, instance.onResourceClose)
			if err != nil {
				go instance.onError(err)
				return err
			}
			instance.leverResources[leverResource] = resource
			return nil
		}

		return resource.KeepAlive()
	}

	return nil
}

// InstanceAddr returns the IP + port the instance is accessible at for Lever
// RPCs.
func (instance *LeverInstance) InstanceAddr() string {
	return instance.instanceAddr
}

func (instance *LeverInstance) onExpired() {
	instance.lock.Lock()
	defer instance.lock.Unlock()
	instance.logger.Info("Instance expired")
	instance.closeInternal(false)
}

func (instance *LeverInstance) registerAsService(proxyInAddr string) {
	instanceExpiry := InstanceExpiryTimeFlag.Get()
	err := scale.RegisterServiceLocal(
		instance.servingID, instance.instanceID, proxyInAddr, instanceExpiry)
	if err != nil {
		instance.logger.WithFields("err", err).Error(
			"Error registering instance service with Consul")
		instance.Close(true)
		return
	}
	instance.serviceSKA = scale.NewServiceSelfKeepAlive(
		instance.instanceID, 15*time.Second)
}

func (instance *LeverInstance) monitorContainer() {
	exitCode, err := instance.docker.WaitContainer(instance.containerID)
	if err != nil {
		instance.onError(err)
		return
	}

	instance.lock.Lock()
	defer instance.lock.Unlock()
	if instance.closing {
		return
	}
	instance.logger.WithFields("exitCode", exitCode).Info("Container exited")
	instance.closeInternal(true)
}

func (instance *LeverInstance) monitorInstanceLock(levInstResourceID string) {
	err := scale.WaitResource(leverutil.ServiceFlag.Get(), levInstResourceID)
	if err != nil {
		instance.logger.WithFields("err", err).Debug("Instance lock error")
	}

	instance.lock.Lock()
	defer instance.lock.Unlock()
	if instance.closing {
		return
	}
	instance.logger.Info("Instance lock expired")
	instance.closeInternal(true)
}

func (instance *LeverInstance) onResourceClose(resourceName string, err error) {
	instance.lock.Lock()
	delete(instance.leverResources, resourceName)
	instance.lock.Unlock()

	if err != nil {
		instance.onError(err)
	}
}

func (instance *LeverInstance) onError(err error) {
	instance.logger.WithFields("err", err).Error("Encountered error")
	instance.lock.Lock()
	instance.closeInternal(true)
	instance.lock.Unlock()
}

// Close shuts down the instance.
func (instance *LeverInstance) Close(kill bool) {
	instance.lock.Lock()
	instance.closeInternal(kill)
	instance.lock.Unlock()
}

func (instance *LeverInstance) closeInternal(kill bool) {
	if instance.closing {
		return
	}
	instance.closing = true

	// Remove the resources.
	for _, resource := range instance.leverResources {
		resource.SoftClose()
	}

	instance.expiryTimer.Stop()
	if instance.sessionKAB != nil {
		instance.sessionKAB.Close()
	}
	instance.serviceSKA.Stop()

	err := instance.termContainer(kill)
	if err != nil {
		instance.logger.WithFields("err", err).Debug("Kill error")
	}

	// Async so as not to hog the lock.
	go func() {
		err = scale.DeregisterService(instance.instanceID)
		if err != nil {
			instance.logger.WithFields("err", err).Error(
				"Service deregister error")
		}
		if instance.levInstResource != nil {
			err = instance.levInstResource.Deregister()
			if err != nil {
				instance.logger.WithFields("err", err).Debug(
					"Instance lock deregister error")
			}
		}
	}()

	go instance.onCloseFun(instance.instanceID, nil)
}

func (instance *LeverInstance) termContainer(kill bool) error {
	var sig dockerapi.Signal
	if kill {
		sig = dockerapi.SIGKILL
	} else {
		sig = dockerapi.SIGTERM
	}
	err := instance.docker.KillContainer(
		dockerapi.KillContainerOptions{
			ID:     instance.containerID,
			Signal: sig,
		})
	if err != nil {
		return err
	}
	exited := make(chan struct{})
	go func() {
		instance.docker.WaitContainer(instance.containerID)
		err := dockerutil.RemoveDockerContainer(
			instance.docker, instance.containerID)
		if err != nil {
			instance.logger.WithFields(
				"err", err,
				"containerID", instance.containerID,
			).Error("Error trying to remove container after it exited")
		}
		close(exited)
	}()
	if !kill {
		go func() {
			timer := time.NewTimer(10 * time.Second)
			select {
			case <-timer.C:
				instance.logger.Info(
					"Instance did not exit on term signal and had to be killed")
				instance.docker.KillContainer(
					dockerapi.KillContainerOptions{
						ID:     instance.containerID,
						Signal: dockerapi.SIGKILL,
					})
			case <-exited:
			}
		}()
	}
	return nil
}
