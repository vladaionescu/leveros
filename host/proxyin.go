package host

import (
	"fmt"
	"strconv"
	"strings"
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
		headers,
		"lever-url",
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

	leverURL, err := core.ParseLeverURL(headers["lever-url"][0])
	if err != nil {
		proxy.inLogger.WithFields(
			"err", err, "leverURL", headers["lever-url"][0]).Error(
			"Unable to parse Lever URL")
	}

	if !core.IsInternalEnvironment(leverURL.Environment) {
		err = fmt.Errorf("Cannot route to dest env")
		proxy.inLogger.WithFields(
			"err", err,
			"leverEnv", leverURL.Environment,
		).Error("")
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
			proxy.inLogger.WithFields(
				"err", err,
				"leverEnv", leverURL.Environment,
			).Error("Could not find an instanceID for provided servingID")
			stream.Write(&http2stream.MsgError{Err: err})
			return
		}
	}

	streamID := leverutil.RandomID()
	proxy.inLogger.WithFields(
		"leverURL", leverURL.String(),
		"srcEnv", srcEnv,
		"leverInstanceID", instanceID,
		"containerID", containerID,
		"servingID", servingID,
		"levInstSessionID", levInstSessionID,
		"levInstResourceID", levInstResourceID,
		"streamID", streamID,
	).Debug("Receiving stream")
	streamLogger := proxy.inLogger.WithFields("streamID", streamID)

	if !core.IsInternalEnvironment(leverURL.Environment) {
		err = fmt.Errorf("Environment not routable internally")
		streamLogger.WithFields(
			"err", err,
			"leverEnv", leverURL.Environment,
		).Error("")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	networkIP, ownIP, instanceAddr, keepAliveFun, err :=
		proxy.manager.EnsureInfrastructureInitialized(&hostman.InstanceInfo{
			Environment:       leverURL.Environment,
			Service:           leverURL.Service,
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

	err = proxy.serveOut(leverURL.Environment, networkIP)
	if err != nil {
		streamLogger.WithFields("err", err).Error(
			"Error listening on env network")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	destStreamI, err := leverutil.ExpBackoff(
		func() (clientStream interface{}, err error, finalErr error) {
			clientStream, err = proxy.client.NewStream(instanceAddr)
			if err != nil {
				if strings.Contains(err.Error(), "connection refused") &&
					proxy.manager.IsInstanceAlive(servingID, instanceID) {
					// Retry.
					return nil, err, nil
				}
				return nil, nil, err
			}
			return clientStream, nil, nil
		}, 10*time.Millisecond, 15*time.Second)
	if err != nil {
		streamLogger.WithFields(
			"err", err,
			"instanceAddr", instanceAddr,
		).Error("Error trying to create client stream to dest")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}
	destStream := destStreamI.(*http2stream.HTTP2Stream)

	addHeaders := make(map[string][]string)
	if srcEnv != "" {
		addHeaders["x-lever-src-env"] = []string{srcEnv}
	}
	addHeaders["x-lever-internal-rpc-gateway"] = []string{ownIP}

	startTime := time.Now()
	firstHeaders := true
	stream.ProxyTo(
		destStream,
		func(msg http2stream.MsgItem) []http2stream.MsgItem {
			proxy.client.KeepAlive(instanceAddr)
			keepAliveFun(
				leverURL.Resource, levResResourceID, levResSessionID)
			return proxy.filterTo(&firstHeaders, addHeaders, msg)
		},
		func(msg http2stream.MsgItem) []http2stream.MsgItem {
			proxy.client.KeepAlive(instanceAddr)
			keepAliveFun(
				leverURL.Resource, levResResourceID, levResSessionID)
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
		Environment: leverURL.Environment,
		Service:     leverURL.Service,
		ServingID:   servingID,
		CodeVersion: codeVersion,
		IsAdmin:     core.IsAdmin(leverURL),
		RpcNanos:    rpcNanos,
	})
	if err != nil {
		streamLogger.WithFields("err", err).Error(
			"Failed to send RPC stats to fleettracker")
	}
}
