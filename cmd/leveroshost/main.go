package main

import (
	"net"

	"github.com/leveros/leveros/cmd"
	"github.com/leveros/leveros/config"
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

var logger = leverutil.GetLogger(PackageName, "main")

var (
	// InternalGRPCListenPortFlag is the port to listen to for internal GRPC
	// connections.
	InternalGRPCListenPortFlag = config.DeclareString(
		PackageName, "internalGRPCListenPort", "3501")
)

func main() {
	config.Initialize()
	leverutil.UpdateLoggingSettings()

	// Docker.
	dockerLocal := leverutil.NewDockerLocal()
	dockerSwarm := leverutil.NewDockerSwarm()

	// Own regional IP.
	ownIP, err := leverutil.GetOwnEnvIPv4(
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

	// GRPC enter loop.
	err = grpcServer.Serve(listener)
	if err != nil {
		logger.WithFields("err", err).Fatal("Error in internal GRPC server")
	}

	// TODO: Ideally we should trap signals and shut down properly.
}
