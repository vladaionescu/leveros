package main

import (
	"net"

	"github.com/leveros/leveros/apiserver"
	"github.com/leveros/leveros/cmd"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/devlogger"
	"github.com/leveros/leveros/dockerutil"
	"github.com/leveros/leveros/fleettracker"
	"github.com/leveros/leveros/host"
	"github.com/leveros/leveros/hostman"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/scale"
	"github.com/leveros/leveros/store"
	"google.golang.org/grpc"
)

// PackageName is the name of this package.
const PackageName = cmd.PackageName + ".leveroshost"

var (
	// Version is the version of Lever OS. This variable is set at build time.
	Version string
	// GitHash is the git hash of Lever OS. This variable is set at build time.
	GitHash string
)

var logger = leverutil.GetLogger(PackageName, "main")

var (
	// InternalGRPCListenPortFlag is the port to listen to for internal GRPC
	// connections.
	InternalGRPCListenPortFlag = config.DeclareString(
		PackageName, "internalGRPCListenPort", "3501")
	// ExternalIPFlag is the host's external IP address.
	ExternalIPFlag = config.DeclareString(
		PackageName, "externalIP", "127.0.0.1")
)

func main() {
	config.Initialize()
	leverutil.UpdateLoggingSettings()

	logger.WithFields(
		"version", Version,
		"gitHash", GitHash,
	).Info("Starting up...")

	_, err := devlogger.NewDevLogger(ExternalIPFlag.Get())
	if err != nil {
		logger.WithFields("err", err).Fatal("Cannot start devlogger")
	}

	// Docker.
	dockerLocal := dockerutil.NewDockerLocal()
	dockerSwarm := dockerutil.NewDockerSwarm()

	// Own regional IP.
	ownIP, err := dockerutil.GetOwnEnvIPv4(
		dockerLocal, hostman.RegionalNetworkFlag.Get())
	if err != nil {
		logger.WithFields("err", err).Fatal("Error getting own regional IPv4")
	}
	// Own GRPC address.
	grpcAddr := ownIP + ":" + InternalGRPCListenPortFlag.Get()

	// Start internal GRPC server.
	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.WithFields(
			"err", err,
			"listenAddr", grpcAddr,
		).Fatal("Could not start listening")
	}
	grpcServer := grpc.NewServer()

	// GRPC client pool.
	grpcPool, err := scale.NewGRPCPool()
	if err != nil {
		logger.WithFields("err", err).Fatal("Cannot create GRPC pool")
	}

	// Start finder.
	as, err := store.NewAerospike()
	if err != nil {
		logger.WithFields("err", err).Fatal("Cannot get aerospike client")
	}
	finder := host.NewFinder(dockerSwarm, as)

	// Start host manager.
	proxyInAddr := ownIP + ":" + host.EnvInListenPortFlag.Get()
	manager, err := hostman.NewManager(
		grpcServer, grpcPool, dockerLocal, grpcAddr, proxyInAddr)
	if err != nil {
		logger.WithFields("err", err).Fatal("Cannot create host manager")
	}

	// Start proxy.
	_, err = host.NewLeverProxy(manager, finder, ownIP, grpcPool)
	if err != nil {
		logger.WithFields("err", err).Fatal("Cannot create Lever proxy")
	}

	// Start fleettracker.
	_, err = fleettracker.NewFleetTracker(
		grpcServer, grpcPool, dockerSwarm, grpcAddr)
	if err != nil {
		logger.WithFields("err", err).Fatal("Error starting fleettracker")
	}

	// Start API server.
	_, err = apiserver.NewServer()
	if err != nil {
		logger.WithFields("err", err).Fatal("Error starting API server")
	}

	logger.Info("\n" +
		"    _                       ___  ___ \n" +
		"   | |   _____ _____ _ _   / _ \\/ __|\n" +
		"   | |__/ -_) V / -_) '_| | (_) \\__ \\\n" +
		"   |____\\___|\\_/\\___|_|    \\___/|___/\n" +
		"   v" + Version + "\n" +
		"                       Ready to serve\n")

	// GRPC enter loop.
	err = grpcServer.Serve(listener)
	if err != nil {
		logger.WithFields("err", err).Fatal("Error in internal GRPC server")
	}

	// TODO: Ideally we should trap signals and shut down properly.
}
