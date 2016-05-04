package dockerutil

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	dockerapi "github.com/fsouza/go-dockerclient"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/devlogger"
	"github.com/leveros/leveros/leverutil"
)

// PackageName is the name of this package.
const PackageName = "dockerutil"

var (
	// DockerSwarmFlag is the docker swarm endpoint.
	DockerSwarmFlag = config.DeclareString(
		PackageName, "dockerSwarm", "unix:///var/run/docker.sock")
	// DockerLocalFlag is the local docker endpoint.
	DockerLocalFlag = config.DeclareString(
		PackageName, "dockerLocal", "unix:///var/run/docker.sock")

	// LeverCodeHostDirFlag is the location on the host (outside docker) of the
	// mounted customer code directory, containing all code from all
	// environments and services.
	LeverCodeHostDirFlag = config.DeclareString(
		PackageName, "leverCodeHostDir", "/tmp/leveros/custcodetree")

	// DisableRemoveContainerFlag causes Lever containers to not be
	// removed (on errors, on exit etc).
	DisableRemoveContainerFlag = config.DeclareBool(
		PackageName, "disableRemoveContainer")
)

var logger = leverutil.GetLogger(PackageName, "dockerutil")

var (
	ownContainerIDOnce sync.Once
	ownContainerID     = ""
)

// HostCodeDirPath returns the path on the docker host that the code for a given
// service version is found at.
func HostCodeDirPath(environment, service string, codeVersion int64) string {
	return filepath.Join(
		LeverCodeHostDirFlag.Get(), environment, service,
		strconv.Itoa(int(codeVersion)))
}

// CodeDirPath returns the path within the docker container that the code
// for a given service version is found at.
func CodeDirPath(environment, service string, codeVersion int64) string {
	return filepath.Join(
		"/leveros/custcodetree", environment, service,
		strconv.Itoa(int(codeVersion)))
}

// GetOwnContainerID returns the container ID of the currently running
// container. This assumes access to the local docker.
func GetOwnContainerID() string {
	ownContainerIDOnce.Do(func() {
		docker := NewDockerLocal()
		containers, err := docker.ListContainers(
			dockerapi.ListContainersOptions{
				Filters: map[string][]string{
					"status": {"running"},
					"label":  {"com.leveros.isleveros"},
				},
			})
		if err != nil {
			logger.WithFields("err", err).Fatal("Cannot get own container ID")
		}

		ownName := "/" + leverutil.ContainerNameFlag.Get()
		for _, container := range containers {
			for _, name := range container.Names {
				if name == ownName {
					ownContainerID = container.ID
					logger.WithFields(
						"containerID", ownContainerID,
					).Info("Detected own container ID")
					return
				}
			}
		}

		logger.WithFields("err", err, "containerName", ownName).Fatal(
			"Cannot find own container ID by name")
	})
	return ownContainerID
}

// NewDockerSwarm returns a client for the docker swarm.
func NewDockerSwarm() (docker *dockerapi.Client) {
	docker, err := dockerapi.NewClient(DockerSwarmFlag.Get())
	if err != nil {
		logger.WithFields("err", err).Fatal(
			"Failed to create swarm docker client")
	}
	return
}

// NewDockerLocal returns a client for the local docker.
func NewDockerLocal() (docker *dockerapi.Client) {
	docker, err := dockerapi.NewClient(DockerLocalFlag.Get())
	if err != nil {
		logger.WithFields("err", err).Fatal(
			"Failed to create docker client for local")
	}
	return
}

// StartDockerContainer starts a new Lever container for the specified
// environment and service.
func StartDockerContainer(
	docker *dockerapi.Client, environment string, service string,
	instanceID string, codeVersion int64, isAdmin bool,
	leverConfig *core.LeverConfig) (
	containerID string, node string, err error) {
	codeDir := HostCodeDirPath(environment, service, codeVersion)
	binds := []string{codeDir + ":/leveros/custcode:ro,Z"}
	env := []string{
		"LEVEROS_ENVIRONMENT=" + environment,
		"LEVEROS_SERVICE=" + service,
		"LEVEROS_INSTANCE_ID=" + instanceID,
		"LEVEROS_CODE_VERSION=" + strconv.Itoa(int(codeVersion)),
		"LEVEROS_INTERNAL_ENV_SUFFIX=" +
			core.InternalEnvironmentSuffixFlag.Get(),
	}
	if isAdmin {
		// This is used by admin to make deployments.
		binds = append(
			binds, LeverCodeHostDirFlag.Get()+":/leveros/custcodetree:Z")
	}

	// Configure logging.
	var logConfig dockerapi.LogConfig
	if devlogger.DisableFlag.Get() {
		logConfig.Type = "none"
	} else {
		// TODO: Should use scale.Dereference... to get IP of syslog server
		//       and shard by env+service.
		tag := fmt.Sprintf(
			"%s/%s/%d/%s", environment, service, codeVersion, instanceID)
		logConfig.Type = "syslog"
		logConfig.Config = map[string]string{
			"syslog-address":  "tcp://127.0.0.1:6514",
			"syslog-facility": "user",
			"tag":             tag,
			"syslog-format":   "rfc5424",
		}
	}

	memoryBytes := int64(leverConfig.InstanceMemoryMB) * 1000 * 1000
	memAndSwapBytes := memoryBytes     // No swap.
	kernelMemBytes := memoryBytes / 10 // Allow 10% memory for kernel.

	// Entry point.
	entry := leverConfig.EntryPoint
	if leverConfig.JSEntryPoint != "" {
		// Trigger GC in node when garbage reaches 90% of memory.
		maxGarbage := strconv.Itoa(
			int(float32(leverConfig.InstanceMemoryMB) * 0.9))
		// Set entry point for node.
		entry = []string{
			"node", "--optimize_for_size", "--max_old_space_size=" + maxGarbage,
			"--gc_interval=100",
			"/leveros/js/leveros-server/compiled/lib/serve.js",
			leverConfig.JSEntryPoint,
		}
	}

	container, err := docker.CreateContainer(dockerapi.CreateContainerOptions{
		Name: "leveros_" + instanceID,
		Config: &dockerapi.Config{
			Image:        "leveros/levercontainer:latest",
			Cmd:          entry,
			Env:          env,
			KernelMemory: kernelMemBytes,
			Labels: map[string]string{
				"com.leveros.environment": environment,
				"com.leveros.service":     service,
				"com.leveros.instanceid":  instanceID,
				"com.leveros.codeversion": strconv.Itoa(int(codeVersion)),
			},
		},
		// TODO: Documentation for these here:
		//       https://docs.docker.com/engine/reference/api/docker_remote_api_v1.23/#create-a-container
		// TODO: Also check if need to set limits on IO operations (blkio).
		// TODO: Should allow to write to disk, but limit amount of disk space.
		HostConfig: &dockerapi.HostConfig{
			ReadonlyRootfs:   true,
			Binds:            binds,
			CapDrop:          []string{"all"},
			NetworkMode:      "none",
			Ulimits:          []dockerapi.ULimit{},          // TODO
			SecurityOpt:      []string{"no-new-privileges"}, // TODO
			LogConfig:        logConfig,
			Memory:           memoryBytes,
			MemorySwap:       memAndSwapBytes,
			MemorySwappiness: 0,
			CPUShares:        0, // TODO
			CPUPeriod:        0, // TODO
			CPUQuota:         0, // TODO
		},
	})
	if err != nil {
		logger.WithFields("err", err).Debug(
			"Error trying to create container")
		return "", "", err
	}

	// Get info about the node it was allocated to by Docker Swarm.
	container, err = docker.InspectContainer(container.ID)
	if err != nil {
		removeErr := RemoveDockerContainer(docker, container.ID)
		if removeErr != nil {
			logger.WithFields(
				"containerID", containerID,
				"err", removeErr,
			).Error("Error trying to remove container after previous error")
		}
		return "", "", err
	}
	if container.Node != nil {
		node = container.Node.Name
	} else {
		// In a dev/testing (non-swarm) environment.
		logger.Warning(
			"Using non-swarm node. " +
				"YOU SHOULD NEVER SEE THIS IN PRODUCTION.")
		node = "leverosconsul"
	}

	// Start the container.
	err = docker.StartContainer(container.ID, nil)
	if err != nil {
		removeErr := RemoveDockerContainer(docker, container.ID)
		if removeErr != nil {
			logger.WithFields(
				"containerID", containerID,
				"err", removeErr,
			).Error("Error trying to remove container after failed to start")
		}
		return "", "", err
	}

	// Need to disconnect it from the "none" network before being able to
	// connect it to its local environment network.
	err = DisconnectFromDockerEnvNetwork(docker, container.ID, "none")
	if err != nil {
		removeErr := RemoveDockerContainer(docker, container.ID)
		if removeErr != nil {
			logger.WithFields(
				"containerID", containerID,
				"err", removeErr,
			).Error("Error trying to remove container after previous error")
		}
		return "", "", err
	}

	return container.ID, node, nil
}

// RemoveDockerContainer removes a Docker container which has stopped.
func RemoveDockerContainer(docker *dockerapi.Client, containerID string) error {
	if DisableRemoveContainerFlag.Get() {
		logger.Warning(
			"disableRemoveContainer flag is true. " +
				"YOU SHOULD NEVER SEE THIS IN PRODUCTION!")
		return nil
	}
	return docker.RemoveContainer(dockerapi.RemoveContainerOptions{
		ID:            containerID,
		Force:         true,
		RemoveVolumes: true,
	})
}

// CreateDockerEnvNetwork creates the network bridge for an environment. Returns
// the network IPv4 of the bridge (useful for listening for connections from
// that bridge) as well as our own IPv4 on that network.
func CreateDockerEnvNetwork(
	docker *dockerapi.Client, environment string) (
	networkID string, networkIPv4 string, ownIPv4 string, err error) {
	// Create.
	networkName := getNetworkName(environment)
	network, err := docker.CreateNetwork(dockerapi.CreateNetworkOptions{
		Name:           networkName,
		Driver:         "bridge",
		CheckDuplicate: true,
	})
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// We already have this network. No need to create it.
			network, err = docker.NetworkInfo(networkName)
			if err != nil {
				return "", "", "", err
			}
		} else {
			return "", "", "", err
		}
	} else {
		// Get more info.
		network, err = docker.NetworkInfo(network.ID)
		if err != nil {
			return "", "", "", err
		}
	}
	if len(network.IPAM.Config) == 0 {
		return "", "", "", fmt.Errorf("Network has no config")
	}

	// Connect ourselves to the network.
	ownIPv4, err = ConnectToDockerEnvNetwork(
		docker, GetOwnContainerID(), network.ID)
	if err != nil {
		docker.RemoveNetwork(network.ID)
		return "", "", "", err
	}

	networkIPv4 = strings.Split(network.IPAM.Config[0].Subnet, "/")[0]
	return network.ID, networkIPv4, ownIPv4, nil
}

// RemoveDockerEnvNetwork removes the network bridge for an environment.
func RemoveDockerEnvNetwork(
	docker *dockerapi.Client, networkID string) error {
	// Disconnect ourselves.
	err := DisconnectFromDockerEnvNetwork(
		docker, GetOwnContainerID(), networkID)
	if err != nil {
		return err
	}
	// Remove.
	return docker.RemoveNetwork(networkID)
}

// ConnectToDockerEnvNetwork connects the provided container ID to the network
// bridge of an environment and returns the IPv4 of the container within that
// network.
func ConnectToDockerEnvNetwork(
	docker *dockerapi.Client, containerID, networkID string) (
	containerIPv4 string, err error) {
	err = docker.ConnectNetwork(
		networkID, dockerapi.NetworkConnectionOptions{Container: containerID})
	if err != nil {
		return "", err
	}

	return GetDockerEnvIPv4(docker, containerID, networkID)
}

// DisconnectFromDockerEnvNetwork disconnects the provided container ID from the
// network bridge of an environment.
func DisconnectFromDockerEnvNetwork(
	docker *dockerapi.Client, containerID, networkID string) error {
	return docker.DisconnectNetwork(
		networkID, dockerapi.NetworkConnectionOptions{Container: containerID})
}

// GetDockerEnvIPv4 returns the IPv4 of a container within the network bridge
// of an environment.
func GetDockerEnvIPv4(
	docker *dockerapi.Client, containerID, networkID string) (string, error) {
	network, err := docker.NetworkInfo(networkID)
	if err != nil {
		return "", err
	}
	endpoint, ok := network.Containers[containerID]
	if !ok {
		return "", fmt.Errorf("Container not found")
	}
	return strings.Split(endpoint.IPv4Address, "/")[0], nil
}

// GetOwnEnvIPv4 returns the IPv4 address within the network bridge of an
// environment for the current process.
func GetOwnEnvIPv4(
	docker *dockerapi.Client, networkID string) (string, error) {
	return GetDockerEnvIPv4(docker, GetOwnContainerID(), networkID)
}

func getNetworkName(env string) string {
	return "leveros_" + env
}
