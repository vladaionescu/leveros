package core

import (
	"os"
	"strings"

	grpc "github.com/leveros/grpc-go"
	"github.com/leveros/leveros/config"
)

// PackageName is the name of this package.
const PackageName = "core"

const (
	// RPCMethodHandler represents the gRPC method handling Lever RPCs.
	RPCMethodHandler = "HandleRPC"
	// StreamingRPCMethodHandler represents the gRPC method handling streaming
	// Lever RPCs.
	StreamingRPCMethodHandler = "HandleStreamingRPC"
)

var (
	// InstanceListenPortFlag is the port Lever instances listen on for
	// Lever RPCs.
	InstanceListenPortFlag = config.DeclareString(
		PackageName, "instanceListenPort", "3837")
	// InternalEnvironmentSuffixFlag represents the ending of the environment
	// host name to which RPCs can be routed to directly (via internal proxies).
	InternalEnvironmentSuffixFlag = config.DeclareString(
		PackageName, "internalEnvSufix", ".lever")
	// EnvAliasMapFlag is a comma-separated mapping of incoming envs to
	// translated env names that will be used internally for routing. Useful
	// in development, when we want to test against a localhost server.
	EnvAliasMapFlag = config.DeclareString(
		PackageName, "envAliasMap", getDefaultLeverOSIPPort()+",dev.lever")

	// DefaultDevAliasFlag is the actual address of the default Lever
	// environment used for local development.
	DefaultDevAliasFlag = config.DeclareString(
		PackageName, "defaultDevAlias", getDefaultLeverOSIPPort())
	// DefaultDevEnvFlag is the default Lever environment used for local
	// development.
	DefaultDevEnvFlag = config.DeclareString(
		PackageName, "defaultDevEnv", "dev.lever")
	// AdminEnvFlag is the admin Lever environment.
	AdminEnvFlag = config.DeclareString(
		PackageName, "adminEnv", "admin.lever")
)

func getDefaultLeverOSIPPort() string {
	ipPort := os.Getenv("LEVEROS_IP_PORT")
	if ipPort != "" {
		return ipPort
	}
	return "127.0.0.1:8080"
}

// IsInternalEnvironment returns true iff the provided environment is part of
// the same Lever deployment (RPCs can be routed internally).
func IsInternalEnvironment(environment string) bool {
	suffix := InternalEnvironmentSuffixFlag.Get()
	if suffix == "" {
		return false
	}
	return strings.HasSuffix(environment, suffix)
}

// IsAdmin returns true iff the env + service represent the admin service.
func IsAdmin(environment string, service string) bool {
	return environment == AdminEnvFlag.Get() && service == "admin"
}

// ProcessEnvAlias returns the environment name after looking through the env
// alias map.
func ProcessEnvAlias(env string) (translatedEnv string) {
	// Parse map.
	// TODO: Cache this for faster execution.
	envMapSlice := strings.Split(EnvAliasMapFlag.Get(), ",")
	envMap := make(map[string]string)
	var key string
	expectKey := true
	for _, part := range envMapSlice {
		if expectKey {
			key = part
		} else {
			envMap[key] = part
		}
		expectKey = !expectKey
	}

	translatedEnv, ok := envMap[env]
	if ok {
		return translatedEnv
	}
	return env
}

// NewServiceDesc creates a GRPC service desc with custom ServiceName set as
// Lever <service>/<resource> format.
func NewServiceDesc(service string, resource string) *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: service + "/" + resource,
		HandlerType: (*LeverRPCServer)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "HandleRPC",
				Handler:    _LeverRPC_HandleRPC_Handler,
			},
		},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "HandleStreamingRPC",
				Handler:       _LeverRPC_HandleStreamingRPC_Handler,
				ServerStreams: true,
				ClientStreams: true,
			},
		},
	}
}
