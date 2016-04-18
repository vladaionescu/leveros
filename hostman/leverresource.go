package hostman

import (
	"fmt"
	"sync"
	"time"

	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/scale"
	"golang.org/x/net/context"
)

var (
	// ResourceExpiryTimeFlag is the expiry time after its last use after which
	// a resource is evicted from the host.
	ResourceExpiryTimeFlag = config.DeclareDuration(
		PackageName, "resourceExpiryTime", 3*time.Minute)
)

// LeverResource represents an instance of a Lever resource within a Lever
// service, that is hosted on a Lever instance. It takes care of monitoring the
// Consul lock associated with the resource and manages the resource's
// lifecycle.
type LeverResource struct {
	leverEnv       string
	leverService   string
	leverResource  string
	instanceAddr   string
	levResResource *scale.Resource
	conns          *scale.GRPCPool
	sessionKAB     *scale.KeepAliveBuffer
	expiryTimer    *time.Timer
	expiryDur      time.Duration
	onCloseFun     OnCloseFun
	logger         *leverutil.Logger

	// Guard the following.
	lock    sync.Mutex
	closing bool
}

// NewLeverResource returns a new instance of LeverResource.
func NewLeverResource(
	leverEnv, leverService, instanceID, leverResource, levResResourceID,
	levResSessionID, instanceAddr string,
	conns *scale.GRPCPool, onCloseFun OnCloseFun) (*LeverResource, error) {
	expiryDur := ResourceExpiryTimeFlag.Get()
	logger := leverutil.GetLogger(PackageName, "leverresource").WithFields(
		"leverEnv", leverEnv,
		"leverService", leverService,
		"leverInstanceID", instanceID,
		"leverResource", leverResource,
	)
	resource := &LeverResource{
		leverEnv:      leverEnv,
		leverService:  leverService,
		leverResource: leverResource,
		levResResource: scale.ExistingResource(
			leverutil.ServiceFlag.Get(), levResResourceID, levResSessionID),
		instanceAddr: instanceAddr,
		conns:        conns,
		sessionKAB: scale.NewSessionKeepAliveBuffer(
			levResSessionID, expiryDur),
		expiryDur:  expiryDur,
		onCloseFun: onCloseFun,
		logger:     logger,
	}
	resource.expiryTimer = time.AfterFunc(expiryDur, resource.onExpired)
	go resource.monitorResourceLock(levResResourceID)
	resource.construct()
	return resource, nil
}

// KeepAlive resets the resource's TTL expiry.
func (resource *LeverResource) KeepAlive() error {
	resource.lock.Lock()
	defer resource.lock.Unlock()
	if resource.closing {
		return fmt.Errorf("Resource closing")
	}

	resource.expiryTimer.Reset(resource.expiryDur)
	resource.sessionKAB.KeepAlive()
	return nil
}

func (resource *LeverResource) onExpired() {
	resource.lock.Lock()
	defer resource.lock.Unlock()
	resource.logger.Info("Resource expired")
	resource.closeInternal(false)
}

func (resource *LeverResource) monitorResourceLock(levResResourceID string) {
	err := scale.WaitResource(leverutil.ServiceFlag.Get(), levResResourceID)
	if err != nil {
		resource.logger.WithFields("err", err).Debug("Resource lock error")
	}

	resource.lock.Lock()
	defer resource.lock.Unlock()
	if resource.closing {
		return
	}
	resource.logger.Info("Resource lock expired")
	resource.closeInternal(false)
}

func (resource *LeverResource) construct() error {
	// TODO: Is it ok to hold lock while doing RPC within container?
	//       What happens if call takes a really long time?
	// Send RPC to instance to inform that it will be hosting this resource.
	resource.lock.Lock()
	defer resource.lock.Unlock()
	conn, err := resource.conns.Dial(resource.instanceAddr)
	if err != nil {
		resource.logger.WithFields("err", err).Error(
			"Failed to dial in Lever instance")
		return err
	}
	_, err = core.SendLeverRPC(
		conn, context.Background(), resource.leverEnv, resource.leverService,
		"", &core.RPC{
			Method: "NewResource",
			ArgsOneof: &core.RPC_Args{
				Args: &core.JSONArray{
					Element: []*core.JSON{&core.JSON{
						JsonValueOneof: &core.JSON_JsonString{
							JsonString: resource.leverResource,
						},
					}},
				},
			},
		})
	if err != nil {
		resource.logger.WithFields("err", err).Error(
			"Failed resource construct call")
		return err
	}
	return nil
}

// Close closes down the resource.
func (resource *LeverResource) Close() {
	resource.lock.Lock()
	resource.closeInternal(false)
	resource.lock.Unlock()
}

// SoftClose closes down the resource but does not send RPC to container.
func (resource *LeverResource) SoftClose() {
	resource.lock.Lock()
	resource.closeInternal(true)
	resource.lock.Unlock()
}

func (resource *LeverResource) closeInternal(soft bool) {
	if resource.closing {
		return
	}
	resource.closing = true

	resource.expiryTimer.Stop()
	resource.sessionKAB.Close()

	// TODO: Is it ok to hold lock while doing RPC within container?
	//       What happens if call takes a really long time?
	// Send RPC inside container to tell that resource closed.
	if !soft {
		conn, err := resource.conns.Dial(resource.instanceAddr)
		if err != nil {
			resource.logger.WithFields("err", err).Error(
				"Failed to dial in Lever instance")
			go resource.onCloseFun(resource.leverResource, err)
			return
		}
		_, err = core.SendLeverRPC(
			conn, context.Background(), resource.leverEnv,
			resource.leverService, "", &core.RPC{
				Method: "CloseResource",
				ArgsOneof: &core.RPC_Args{
					Args: &core.JSONArray{
						Element: []*core.JSON{&core.JSON{
							JsonValueOneof: &core.JSON_JsonString{
								JsonString: resource.leverResource,
							},
						}},
					},
				},
			})
		if err != nil {
			resource.logger.WithFields("err", err).Error(
				"Failed resource close call")
			go resource.onCloseFun(resource.leverResource, err)
			return
		}
	}

	// Async so as not to hog the lock.
	go func() {
		err := resource.levResResource.Deregister()
		if err != nil {
			resource.logger.WithFields("err", err).Debug(
				"Resource lock deregister error")
		}
	}()

	go resource.onCloseFun(resource.leverResource, nil)
}
