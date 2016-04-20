package host

import (
	"fmt"
	"strconv"
	"time"

	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/fleettracker"
	"github.com/leveros/leveros/hostman"
	"github.com/leveros/leveros/http2stream"
	"github.com/leveros/leveros/leverutil"
)

func (proxy *LeverProxy) serveIn() (err error) {
	listenAddr := proxy.ownIPv4 + ":" + EnvInListenPortFlag.Get()
	proxy.serviceSelfKeepAlive, proxy.sessionSelfKeepAlive, err =
		RegisterThisHost(listenAddr)
	if err != nil {
		return err
	}
	proxy.inLogger.WithFields("listenAddr", listenAddr).Info("Listening")
	proxy.inListener, _, err = proxy.inServer.Serve(
		"tcp", listenAddr, proxy.handleInStream)
	if err != nil {
		proxy.inLogger.WithFields("err", err, "listenAddr", listenAddr).Error(
			"Error trying to listen")
	}
	return
}

func (proxy *LeverProxy) handleInStream(stream *http2stream.HTTP2Stream) {
	headers := stream.GetHeaders()
	err := expectHeaders(
		headers, ":path", "host",
		"x-lever-src-env",
		"x-lever-dest-instance-id",
		"x-lever-dest-container-id",
		"x-lever-serving-id",
		"x-lever-code-version",
		"x-lever-inst-resource-id",
		"x-lever-inst-session-id",
		"x-lever-res-resource-id",
		"x-lever-res-session-id",
	)
	if err != nil {
		proxy.inLogger.WithFields("err", err).Error("")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	serviceName, resourceName, err := parsePath(headers[":path"][0])
	if err != nil {
		proxy.inLogger.WithFields(
			"err", err, "path", headers[":path"][0]).Error(
			"Unable to parse request path")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	destEnv := core.ProcessEnvAlias(headers["host"][0])
	if !core.IsInternalEnvironment(destEnv) {
		err = fmt.Errorf("Cannot route to dest env")
		proxy.inLogger.WithFields("err", err, "leverEnv", destEnv).Error("")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	srcEnv := headers["x-lever-src-env"][0]
	instanceID := headers["x-lever-dest-instance-id"][0]
	containerID := headers["x-lever-dest-container-id"][0]
	servingID := headers["x-lever-serving-id"][0]
	codeVersionInt, err := strconv.Atoi(headers["x-lever-code-version"][0])
	if err != nil {
		proxy.inLogger.WithFields("err", err).Error("Cannot parse code version")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}
	codeVersion := int64(codeVersionInt)
	levInstResourceID := headers["x-lever-inst-resource-id"][0]
	levInstSessionID := headers["x-lever-inst-session-id"][0]
	levResResourceID := headers["x-lever-res-resource-id"][0]
	levResSessionID := headers["x-lever-res-session-id"][0]

	if instanceID == "" {
		instanceID, err = proxy.manager.RandomInstaceID(servingID)
		if err != nil {
			proxy.inLogger.WithFields("err", err, "leverEnv", destEnv).Error(
				"Could not find an instanceID for provided servingID")
			stream.Write(&http2stream.MsgError{Err: err})
			return
		}
	}

	streamID := leverutil.RandomID()
	proxy.inLogger.WithFields(
		"leverEnv", destEnv,
		"leverService", serviceName,
		"leverResource", resourceName,
		"srcEnv", srcEnv,
		"leverInstanceID", instanceID,
		"containerID", containerID,
		"servingID", servingID,
		"levInstSessionID", levInstSessionID,
		"levInstResourceID", levInstResourceID,
		"streamID", streamID,
	).Debug("Receiving stream")
	streamLogger := proxy.inLogger.WithFields("streamID", streamID)

	if !core.IsInternalEnvironment(destEnv) {
		err = fmt.Errorf("Environment not routable internally")
		streamLogger.WithFields("err", err, "leverEnv", destEnv).Error("")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	networkIP, ownIP, instanceAddr, keepAliveFun, err :=
		proxy.manager.EnsureInfrastructureInitialized(&hostman.InstanceInfo{
			Environment:       destEnv,
			Service:           serviceName,
			InstanceID:        instanceID,
			ContainerID:       containerID,
			ServingID:         servingID,
			LevInstResourceID: levInstResourceID,
			LevInstSessionID:  levInstSessionID,
		})
	if err != nil {
		streamLogger.WithFields("err", err).Error(
			"Error initializing instance")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	err = proxy.serveOut(destEnv, networkIP)
	if err != nil {
		streamLogger.WithFields("err", err).Error(
			"Error listening on env network")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	// TODO: If the instance is new, try a few times here. Just in case it takes
	//       longer for the instance to start. (Also, how does the failure look
	//       like in that case?)
	destStream, err := proxy.client.NewStream(instanceAddr)
	if err != nil {
		streamLogger.WithFields(
			"err", err,
			"instanceAddr", instanceAddr,
		).Error("Error trying to create client stream to dest")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	addHeaders := make(map[string][]string)
	if srcEnv != "" {
		addHeaders["x-lever-src-env"] = []string{srcEnv}
	}
	if resourceName != "" {
		addHeaders["x-lever-resource"] = []string{resourceName}
	}
	addHeaders["x-lever-internal-rpc-gateway"] = []string{ownIP}

	startTime := time.Now()
	firstHeaders := true
	stream.ProxyTo(
		destStream,
		func(msg http2stream.MsgItem) []http2stream.MsgItem {
			proxy.client.KeepAlive(instanceAddr)
			keepAliveFun(
				resourceName, levResResourceID, levResSessionID)
			return proxy.filterTo(&firstHeaders, addHeaders, msg)
		},
		func(msg http2stream.MsgItem) []http2stream.MsgItem {
			proxy.client.KeepAlive(instanceAddr)
			keepAliveFun(
				resourceName, levResResourceID, levResSessionID)
			return noFilter(msg)
		})
	// Wait for RPC to finish.
	<-destStream.Closed()
	rpcNanos := uint64(time.Now().Sub(startTime).Nanoseconds())
	streamLogger.WithFields("rpcNanos", rpcNanos).Debug("RPC nanos")

	// Send RPC stats to fleettracker.
	// TODO: For services with high load, we should not send info on every
	//       RPC. They should be batched and sent say... every ~50ms
	//       (well below tracker tick interval).
	err = fleettracker.OnRPC(proxy.grpcPool, &fleettracker.RPCEvent{
		Environment: destEnv,
		Service:     serviceName,
		ServingID:   servingID,
		CodeVersion: codeVersion,
		IsAdmin:     core.IsAdmin(destEnv, serviceName),
		RpcNanos:    rpcNanos,
	})
	if err != nil {
		streamLogger.WithFields("err", err).Error(
			"Failed to send RPC stats to fleettracker")
	}
}
