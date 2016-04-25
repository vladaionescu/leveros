package host

import (
	"fmt"
	"strconv"

	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/http2stream"
	"github.com/leveros/leveros/leverutil"
)

func (proxy *LeverProxy) serveExt() (err error) {
	// TODO: Restrict to external network.
	listenAddr := ":" + EnvExtListenPortFlag.Get()
	proxy.extLogger.WithFields("listenAddr", listenAddr).Info("Listening")
	proxy.extListener, _, err = proxy.inServer.Serve(
		"tcp", listenAddr, func(stream *http2stream.HTTP2Stream) {
			proxy.handleExtOutStream("", stream, true)
		})
	if err != nil {
		proxy.extLogger.WithFields("err", err, "listenAddr", listenAddr).Error(
			"Error trying to listen")
	}
	return
}

func (proxy *LeverProxy) serveOut(env string, networkIP string) (err error) {
	proxy.lock.Lock()
	defer proxy.lock.Unlock()
	_, ok := proxy.outListeners[env]
	if ok {
		// Already listening.
		return nil
	}

	listenAddr := networkIP + ":" + EnvOutListenPortFlag.Get()
	proxy.outLogger.WithFields("listenAddr", listenAddr).Info("Listening")
	outListener, _, err := proxy.outServer.Serve(
		"tcp", listenAddr,
		func(stream *http2stream.HTTP2Stream) {
			proxy.handleExtOutStream(env, stream, false)
		})
	if err != nil {
		return err
	}
	proxy.outListeners[env] = outListener
	return nil
}

// TODO: This is not yet used.
func (proxy *LeverProxy) stopServeOut(env string) (err error) {
	proxy.lock.Lock()
	defer proxy.lock.Unlock()
	outListener, ok := proxy.outListeners[env]
	if !ok {
		return nil
	}
	err = outListener.Close()
	if err != nil {
		return err
	}
	delete(proxy.outListeners, env)
	return nil
}

func (proxy *LeverProxy) handleExtOutStream(
	srcEnv string, stream *http2stream.HTTP2Stream, isExtStream bool) {
	streamLogger := proxy.outLogger
	if isExtStream {
		streamLogger = proxy.extLogger
	}

	headers := stream.GetHeaders()
	err := expectHeaders(headers, "lever-url")
	if err != nil {
		streamLogger.WithFields("err", err).Error("")
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
		streamLogger.WithFields(
			"err", err,
			"leverEnv", leverURL.Environment,
		).Error("")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	streamID := leverutil.RandomID()
	streamLogger.WithFields(
		"leverURL", leverURL.String(),
		"srcEnv", srcEnv,
		"streamID", streamID,
	).Debug("Receiving stream")
	streamLogger = streamLogger.WithFields("streamID", streamID)

	isFromExt := core.IsInternalEnvironment(srcEnv)
	hostInfo, err := proxy.finder.GetHost(
		leverURL.Environment, leverURL.Service, leverURL.Resource, isFromExt)
	if err != nil {
		streamLogger.WithFields(
			"err", err,
			"leverURL", leverURL.String(),
		).Error("Error trying to find / allocate host for request")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	streamLogger.WithFields(
		"leverInstanceID", hostInfo.InstanceID,
		"containerID", hostInfo.ContainerID,
		"servingID", hostInfo.ServingID,
		"hostProxyAddr", hostInfo.HostAddr,
		"isNewInstance", hostInfo.IsNewInstance,
	).Debug("Routing to host proxy")
	destStream, err := proxy.client.NewStream(hostInfo.HostAddr)
	if err != nil {
		streamLogger.WithFields(
			"err", err,
			"hostProxyAddr", hostInfo.HostAddr,
		).Error("Error trying to create client stream to dest")
		stream.Write(&http2stream.MsgError{Err: err})
		return
	}

	addHeaders := make(map[string][]string)
	addHeaders["x-lever-src-env"] = []string{srcEnv}
	addHeaders["x-lever-dest-instance-id"] = []string{hostInfo.InstanceID}
	addHeaders["x-lever-dest-container-id"] = []string{hostInfo.ContainerID}
	addHeaders["x-lever-serving-id"] = []string{hostInfo.ServingID}
	addHeaders["x-lever-code-version"] = []string{
		strconv.Itoa(int(hostInfo.CodeVersion))}
	addHeaders["x-lever-inst-resource-id"] = []string{
		hostInfo.LevInstResourceID}
	addHeaders["x-lever-inst-session-id"] = []string{hostInfo.LevInstSessionID}
	addHeaders["x-lever-res-resource-id"] = []string{hostInfo.LevResResourceID}
	addHeaders["x-lever-res-session-id"] = []string{hostInfo.LevResSessionID}

	firstHeaders := true
	stream.ProxyTo(
		destStream,
		func(msg http2stream.MsgItem) []http2stream.MsgItem {
			proxy.client.KeepAlive(hostInfo.HostAddr)
			return proxy.filterTo(&firstHeaders, addHeaders, msg)
		},
		func(msg http2stream.MsgItem) []http2stream.MsgItem {
			proxy.client.KeepAlive(hostInfo.HostAddr)
			return noFilter(msg)
		})
}
