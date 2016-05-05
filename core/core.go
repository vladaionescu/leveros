// Package core provides common utilities to be used throughout various Lever
// binaries and libraries. These utilities are very specific to Lever.
package core

import (
	"os"
	"strings"

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
func IsAdmin(leverURL *LeverURL) bool {
	return leverURL.Environment == AdminEnvFlag.Get() &&
		leverURL.Service == "admin"
}
