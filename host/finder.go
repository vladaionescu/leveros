package host

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	aerospike "github.com/aerospike/aerospike-client-go"
	dockerapi "github.com/fsouza/go-dockerclient"
	leverapi "github.com/leveros/leveros/api"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/hostman"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/scale"
	"github.com/leveros/leveros/store"
)

var (
	// InstanceConstructTimeoutFlag is the deadline for an instance to spin up.
	InstanceConstructTimeoutFlag = config.DeclareDuration(
		PackageName, "instanceConstructTimeout", 15*time.Second)
	// ResourceConstructTimeoutFlag is the deadline for a resource to get
	// allocated (the time necessary might include spinning up an instance).
	ResourceConstructTimeoutFlag = config.DeclareDuration(
		PackageName, "resourceConstructTimeout", 20*time.Second)
)

// LeverHostInfo is the information returned by a call to GetHost.
type LeverHostInfo struct {
	HostAddr string
	// May be "" if instance was found via DNS.
	InstanceID string
	// May be "" if instance was found via DNS.
	ContainerID   string
	ServingID     string
	CodeVersion   int64
	IsNewInstance bool
	// May be "" if it's non-first instance.
	LevInstResourceID string
	// May be "" if it's non-first instance.
	LevInstSessionID string
	// May be "" if no resource was specified.
	LevResResourceID string
	// May be "" if no resource was specified.
	LevResSessionID string
}

// LevInstTarget is the information associated that is stored with the LevInst
// resource.
// Note: Only the first instance within a service gets a LevInstResourceID and
//       is registered as an internal resource.
type LevInstTarget struct {
	HostAddr    string `json:"ha,omitempty"`
	InstanceID  string `json:"iid,omitempty"`
	ContainerID string `json:"cid,omitempty"`
}

func makeLevInstResourceID(env, service string, version int64) string {
	peer := leverapi.LeverPeer{
		Environment: env,
		Service:     service + "/" + strconv.Itoa(int(version)),
	}
	return peer.String()
}

// LevResTarget is the information associated that is stored with the LevRes
// resource.
type LevResTarget struct {
	HostAddr         string `json:"ha,omitempty"`
	InstanceID       string `json:"iid,omitempty"`
	ContainerID      string `json:"cid,omitempty"`
	LevInstSessionID string `json:"lisid,omitempty"`
}

func makeLevResResourceID(
	env, service, resource string, version int64) string {
	// TODO: Putting the version in the service name means that an upgrade
	//       pretty much causes all resources to be reconstructed in parallel
	//       on the new version. For a while, a given resource is running
	//       in two instances (old code and new code). This can have serious
	//       implications if the customer assumes only 1 instance at a time.
	//       A proper fix for this involves logic for an upgrade coordinator.
	peer := leverapi.LeverPeer{
		Environment: env,
		Service:     service + "/" + strconv.Itoa(int(version)),
		Resource:    resource,
	}
	return peer.String()
}

// RegisterThisHost registers the current swarm node against the current
// service.
func RegisterThisHost(
	hostAddr string) (
	serviceSka *scale.SelfKeepAlive, sessionSka *scale.SelfKeepAlive,
	err error) {
	service := leverutil.ServiceFlag.Get()
	instanceID := leverutil.InstanceIDFlag.Get()
	err = scale.RegisterServiceLocal(
		service, instanceID, hostAddr, 30*time.Second)
	if err != nil {
		return nil, nil, err
	}
	node, err := scale.GetOwnNodeName()
	if err != nil {
		return nil, nil, err
	}
	res, success, err := scale.RegisterResourceLocal(
		"swarm://"+node, 30*time.Second)
	if err != nil {
		return nil, nil, err
	}
	if !success {
		return nil, nil, fmt.Errorf("Duplicate Lever host on this node")
	}
	return scale.NewServiceSelfKeepAlive(instanceID, 15*time.Second),
		scale.NewSessionSelfKeepAlive(res.GetSessionID(), 15*time.Second),
		nil
}

// GetHostAddrOnNode returns the address of the Lever host running on given
// node.
func GetHostAddrOnNode(node string) (string, error) {
	res, err := scale.DereferenceResourceConsistent(
		leverutil.ServiceFlag.Get(), "swarm://"+node)
	if err != nil {
		return "", err
	}
	return res.GetTarget(), nil
}

// Finder is a structure that is able to allocate Lever containers across
// the Lever fleet.
type Finder struct {
	docker *dockerapi.Client
	as     *aerospike.Client
	logger *leverutil.Logger
}

// NewFinder returns a new Finder instance.
func NewFinder(docker *dockerapi.Client, as *aerospike.Client) *Finder {
	return &Finder{
		docker: docker,
		as:     as,
		logger: leverutil.GetLogger(PackageName, "Finder"),
	}
}

// GetHost returns the address of an allocated Lever service of the type
// specified.
func (finder *Finder) GetHost(env, service, resource string, fromExt bool) (
	hostInfo *LeverHostInfo, err error) {
	servingID, version, isPublic, err := store.ServiceServingData(
		finder.as, env, service)
	if err != nil {
		finder.logger.WithFields(
			"err", err,
			"leverEnv", env,
			"leverService", service,
		).Error("Unable to get serving data for service")
		return nil, err
	}
	if fromExt && !isPublic {
		finder.logger.WithFields(
			"leverEnv", env,
			"leverService", service,
		).Error("Accessing private service from external")
		return nil, fmt.Errorf("Not allowed")
	}

	if resource == "" {
		// Lever resource not mentioned. Just find an instance for the
		// service.
		var (
			levInstTarget   *LevInstTarget
			levInstResource *scale.Resource
			isNewInstance   bool
		)
		levInstTarget, _, levInstResource, isNewInstance, err =
			finder.getInstance(env, service, servingID, version)
		if err != nil {
			return nil, err
		}
		var levInstResourceID, levInstSessionID string
		if levInstResource != nil {
			levInstResourceID = makeLevInstResourceID(env, service, version)
			levInstSessionID = levInstResource.GetSessionID()
		}

		return &LeverHostInfo{
			HostAddr:          levInstTarget.HostAddr,
			InstanceID:        levInstTarget.InstanceID,
			ContainerID:       levInstTarget.ContainerID,
			ServingID:         servingID,
			CodeVersion:       version,
			IsNewInstance:     isNewInstance,
			LevInstResourceID: levInstResourceID,
			LevInstSessionID:  levInstSessionID,
		}, nil
	}

	// Lever resource mentioned. Find the instance that serves that resource.
	// (Or allocate one if not yet registered).
	levResTarget, levResResource, isNewInstance, err := finder.getResource(
		env, service, servingID, resource, version)
	if err != nil {
		return nil, err
	}

	var levInstResourceID string
	if levResTarget.LevInstSessionID != "" {
		levInstResourceID = makeLevInstResourceID(env, service, version)
	}

	levResResourceID := makeLevResResourceID(env, service, resource, version)
	return &LeverHostInfo{
		HostAddr:          levResTarget.HostAddr,
		InstanceID:        levResTarget.InstanceID,
		ContainerID:       levResTarget.ContainerID,
		ServingID:         servingID,
		CodeVersion:       version,
		IsNewInstance:     isNewInstance,
		LevInstResourceID: levInstResourceID,
		LevInstSessionID:  levResTarget.LevInstSessionID,
		LevResResourceID:  levResResourceID,
		LevResSessionID:   levResResource.GetSessionID(),
	}, nil
}

func (finder *Finder) getResource(
	env, service, servingID, resource string, version int64) (
	levResTarget *LevResTarget, levResResource *scale.Resource,
	isNewInstance bool, err error) {
	levResResourceID := makeLevResResourceID(env, service, resource, version)

	// See if resource already being served somewhere.
	levResResource, err = scale.DereferenceResource(
		leverutil.ServiceFlag.Get(), levResResourceID)
	if err != nil {
		finder.logger.WithFields("err", err).Error(
			"Could not dereference Lever resource")
		return nil, nil, false, err
	}
	if levResResource == nil {
		// Resource not allocated. Register new one.
		return finder.newResource(env, service, servingID, resource, version)
	}

	levResTarget = new(LevResTarget)
	err = json.Unmarshal([]byte(levResResource.GetTarget()), levResTarget)
	if err != nil {
		finder.logger.WithFields("err", err).Panic("Failed to decode json")
	}
	finder.logger.WithFields(
		"leverEnv", env,
		"leverService", service,
		"leverResource", resource,
		"leverInstanceID", levResTarget.InstanceID,
	).Debug("Reusing resource")
	return levResTarget, levResResource, false, nil
}

func (finder *Finder) newResource(
	env, service, servingID, resource string, version int64) (
	levResTarget *LevResTarget, levResResource *scale.Resource,
	isNewInstance bool, err error) {
	levResResourceID := makeLevResResourceID(env, service, resource, version)

	var success bool
	levResResource, success, err = scale.ConstructResource(
		leverutil.ServiceFlag.Get(), levResResourceID,
		ResourceConstructTimeoutFlag.Get())
	if err != nil {
		finder.logger.WithFields("err", err).Error(
			"Failed to construct resource")
		return nil, nil, false, err
	}
	if !success {
		// Failed to get lock. Someone else constructed resource.
		levResTarget = new(LevResTarget)
		err = json.Unmarshal([]byte(levResResource.GetTarget()), levResTarget)
		if err != nil {
			finder.logger.WithFields("err", err).Panic("Failed to decode json")
		}
		finder.logger.WithFields(
			"leverEnv", env,
			"leverService", service,
			"leverResource", resource,
			"leverInstanceID", levResTarget.InstanceID,
		).Debug("Reusing resource")
		return levResTarget, levResResource, false, nil
	}

	levInstTarget, targetNode, levInstResource, isNewInstance, err :=
		finder.getInstance(env, service, servingID, version)
	if err != nil {
		return nil, nil, false, err
	}

	finder.logger.WithFields(
		"leverEnv", env,
		"leverService", service,
		"leverResource", resource,
		"leverInstanceID", levInstTarget.InstanceID,
	).Debug("Registering new resource")

	var levInstSessionID string
	if levInstResource != nil {
		levInstSessionID = levInstResource.GetSessionID()
	}
	levResTarget = &LevResTarget{
		HostAddr:         levInstTarget.HostAddr,
		ContainerID:      levInstTarget.ContainerID,
		InstanceID:       levInstTarget.InstanceID,
		LevInstSessionID: levInstSessionID,
	}
	target, err := json.Marshal(levResTarget)
	if err != nil {
		finder.logger.WithFields("err", err).Panic("Failed to encode json")
	}
	err = levResResource.DoneConstructing(
		string(target), targetNode, 2*hostman.ResourceExpiryTimeFlag.Get())
	if err != nil {
		finder.logger.WithFields("err", err).Error(
			"Could not register Lever resource")
		return nil, nil, false, err
	}

	return levResTarget, levResResource, isNewInstance, nil
}

func (finder *Finder) getInstance(
	env, service, servingID string, version int64) (
	levInstTarget *LevInstTarget, targetNode string,
	levInstResource *scale.Resource, isNewInstance bool, err error) {
	// First, try DNS-based as it is faster and provides proper
	// load-balancing.
	hostAddr, targetNode, err := scale.DereferenceService(servingID)
	if err == scale.ErrServiceNotFound {
		// Then try resource-based. This has the disadvantage that it always
		// picks the same instance (the one registered as a resource).
		levInstResourceID := makeLevInstResourceID(env, service, version)
		levInstResource, err = scale.DereferenceResource(
			leverutil.ServiceFlag.Get(), levInstResourceID)
		if err != nil {
			finder.logger.WithFields("err", err).Error(
				"Could not dereference Lever instance")
			return nil, "", nil, false, err
		}
		if levInstResource == nil {
			// No instance found for that service. Bring up a fresh one.
			return finder.newInstance(env, service, version)
		}

		levInstTarget = new(LevInstTarget)
		err = json.Unmarshal([]byte(levInstResource.GetTarget()), levInstTarget)
		if err != nil {
			finder.logger.WithFields("err", err).Panic("Failed to decode json")
		}
		finder.logger.WithFields(
			"leverEnv", env,
			"leverService", service,
			"leverInstanceID", levInstTarget.InstanceID,
		).Debug("Reusing instance")
		return levInstTarget, levInstResource.GetTargetNode(), levInstResource,
			false, nil
	} else if err != nil {
		finder.logger.WithFields("err", err).Error("DNS lookup failed")
		return nil, "", nil, false, err
	}
	return &LevInstTarget{HostAddr: hostAddr}, targetNode, nil, false, nil
}

func (finder *Finder) newInstance(
	env, service string, version int64) (
	levInstTarget *LevInstTarget, targetNode string,
	levInstResource *scale.Resource, success bool, err error) {
	// Note: This process only takes place for the very first instance within
	//       a service. The non-first instances do not have a resource entry
	//       and they are found via service lookup through DNS.
	levInstResourceID := makeLevInstResourceID(env, service, version)
	levInstResource, success, err = scale.ConstructResource(
		leverutil.ServiceFlag.Get(), levInstResourceID,
		InstanceConstructTimeoutFlag.Get())
	if err != nil {
		finder.logger.WithFields("err", err).Error(
			"Failed to construct new instance")
		return nil, "", nil, false, err
	}
	if !success {
		// Failed to get lock. Someone else constructed instance.
		levInstTarget = new(LevInstTarget)
		err = json.Unmarshal([]byte(levInstResource.GetTarget()), levInstTarget)
		if err != nil {
			finder.logger.WithFields("err", err).Panic("Failed to decode json")
		}
		finder.logger.WithFields(
			"leverEnv", env,
			"leverService", service,
			"leverInstanceID", levInstTarget.InstanceID,
		).Debug("Reusing instance")
		return levInstTarget, levInstResource.GetTargetNode(), levInstResource,
			false, nil
	}

	// Read the entry point from the config.
	codeDir := leverutil.CodeDirPath(env, service, version)
	leverConfig, err := core.ReadLeverConfig(codeDir)
	if err != nil {
		finder.logger.WithFields("err", err).Error(
			"Failed to read lever.json")
		return nil, "", nil, false, err
	}
	entryPoint := leverConfig.EntryPoint

	// TODO: If somehow first RPC fails and service no longer
	//       contacted afterwards, then the container remains hanging
	//       forever. Need a way to kill it / make it expire after a
	//       while for this situation.
	//       Idea: Poll docker every ~30s for instances that we are not
	//       handling and register those.
	instanceID := leverutil.RandomID() // TODO: Collisions possible.
	isAdmin := core.IsAdmin(env, service)
	containerID, node, err := leverutil.StartDockerContainer(
		finder.docker, env, service, instanceID, entryPoint, version, isAdmin)
	if err != nil {
		finder.logger.WithFields(
			"err", err,
			"leverEnv", env,
			"leverService", service,
			"leverInstanceID", instanceID,
		).Error("Unable to start container")
		return nil, "", nil, false, err
	}

	hostAddr, err := GetHostAddrOnNode(node)
	if err != nil {
		finder.logger.WithFields("err", err).Error(
			"Failed to get Host addr on node")
		return nil, "", nil, false, err
	}

	finder.logger.WithFields(
		"leverEnv", env,
		"leverService", service,
		"leverInstanceID", instanceID,
		"containerID", containerID,
		"node", node,
	).Debug("Creating new instance")

	levInstTarget = &LevInstTarget{
		HostAddr:    hostAddr,
		InstanceID:  instanceID,
		ContainerID: containerID,
	}
	target, err := json.Marshal(levInstTarget)
	if err != nil {
		finder.logger.WithFields("err", err).Panic("Failed to encode json")
	}

	err = levInstResource.DoneConstructing(
		string(target), node, 2*hostman.InstanceExpiryTimeFlag.Get())
	if err != nil {
		finder.logger.WithFields("err", err).Error(
			"Failed to register new instance")
		return nil, "", nil, false, err
	}

	return levInstTarget, node, levInstResource, true, nil
}
