package host

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/hostman"
	"github.com/leveros/leveros/http2stream"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/scale"
)

var (
	// ConnectionExpiryFlag is the inactivity time after which client
	// connections are closed.
	ConnectionExpiryFlag = config.DeclareDuration(
		PackageName, "connectionExpiry", 5*time.Minute)
	// ConnectionConnectTimeoutFlag is the timeout for client connections to
	// establish.
	ConnectionConnectTimeoutFlag = config.DeclareDuration(
		PackageName, "connectionConnectTimeout", 20*time.Second)

	// ListenProxyExtFlag enables listening to the external interface
	// (regional LB).
	ListenProxyExtFlag = config.DeclareBool(
		PackageName, "listenProxyExt")
	// ListenProxyInOutFlag enables listening to requests going to or coming
	// from a local environment.
	ListenProxyInOutFlag = config.DeclareBool(
		PackageName, "listenProxyInOut")

	// EnvOutListenPortFlag is the environment listen port for outward
	// connections. This port is accessible only from within the env network.
	EnvOutListenPortFlag = config.DeclareString(
		PackageName, "envOutListenPort", "3838")
	// EnvInListenPortFlag is the listen port for inward connections.
	EnvInListenPortFlag = config.DeclareString(
		PackageName, "envInListenPort", "3500")
	// EnvExtListenPortFlag is the listen port for external connections.
	EnvExtListenPortFlag = config.DeclareString(
		PackageName, "envExtListenPort", "3502")
)

// LeverProxy is a proxy server that mediates Lever RPCs from the outside world
// to Lever containers and RPCs between Lever containers. The In / Out
// terminology is relative to the Lever containers (not to the proxy).
type LeverProxy struct {
	// Server for connections via unix socket file for RPC's going *out* of
	// Lever containers.
	outServer *http2stream.HTTP2Server
	// Server for connections from other proxies in the same region. These
	// will be proxied *into* Lever containers.
	inServer *http2stream.HTTP2Server
	// Server for connections from outside the region (from the internetz).
	extServer *http2stream.HTTP2Server
	// Client is used for all types of client-connections (in/out/ext).
	client *http2stream.HTTP2Client

	manager              *hostman.Manager
	finder               *Finder
	ownIPv4              string
	grpcPool             *scale.GRPCPool
	inListener           net.Listener
	extListener          net.Listener
	serviceSelfKeepAlive *scale.SelfKeepAlive
	sessionSelfKeepAlive *scale.SelfKeepAlive
	logger               *leverutil.Logger
	inLogger             *leverutil.Logger
	outLogger            *leverutil.Logger
	extLogger            *leverutil.Logger

	// Guard the following.
	lock sync.Mutex
	// env -> outListener
	outListeners map[string]net.Listener
}

// NewLeverProxy creates a new instance of LeverProxy.
func NewLeverProxy(
	manager *hostman.Manager, finder *Finder, ownIPv4 string,
	grpcPool *scale.GRPCPool) (
	*LeverProxy, error) {
	client, err := http2stream.NewHTTP2Client(
		ConnectionConnectTimeoutFlag.Get(), ConnectionExpiryFlag.Get())
	if err != nil {
		return nil, err
	}
	proxy := &LeverProxy{
		outServer:    http2stream.NewHTTP2Server(),
		inServer:     http2stream.NewHTTP2Server(),
		extServer:    http2stream.NewHTTP2Server(),
		client:       client,
		manager:      manager,
		finder:       finder,
		ownIPv4:      ownIPv4,
		grpcPool:     grpcPool,
		outListeners: make(map[string]net.Listener),
		logger:       leverutil.GetLogger(PackageName, "LeverProxy"),
	}
	proxy.inLogger = proxy.logger.WithFields("listener", "in")
	proxy.outLogger = proxy.logger.WithFields("listener", "out")
	proxy.extLogger = proxy.logger.WithFields("listener", "ext")
	if ListenProxyExtFlag.Get() {
		err = proxy.serveExt()
		if err != nil {
			return nil, err
		}
	}
	if ListenProxyInOutFlag.Get() {
		err = proxy.serveIn()
		if err != nil {
			return nil, err
		}
	}
	return proxy, nil
}

func (proxy *LeverProxy) filterTo(
	firstHeaders *bool, addHeaders map[string][]string,
	msg http2stream.MsgItem) []http2stream.MsgItem {
	headersItem, ok := msg.(*http2stream.MsgHeaders)
	if ok {
		// Make a copy.
		filtered := &http2stream.MsgHeaders{
			Headers:   make(map[string][]string),
			EndStream: headersItem.EndStream,
		}
		for k, v := range headersItem.Headers {
			// Strip x-... headers.
			if len(k) >= 2 && k[:2] == "x-" {
				continue
			}
			// Copy rest.
			filtered.Headers[k] = v
		}

		// Add our own headers.
		if *firstHeaders {
			*firstHeaders = false
			for k, v := range addHeaders {
				filtered.Headers[k] = v
			}
		}

		return []http2stream.MsgItem{filtered}
	}

	return []http2stream.MsgItem{msg}
}

func parsePath(path string) (service, resource string, err error) {
	if path != "" && path[0] == '/' {
		path = path[1:]
	}
	lastSep := strings.LastIndex(path, "/")
	if lastSep == -1 {
		return "", "", fmt.Errorf("Invalid path")
	}
	serviceAndResource := path[:lastSep]

	serviceSep := strings.Index(serviceAndResource, "/")
	if serviceSep == -1 {
		// Resource not mentioned.
		return serviceAndResource, "", nil
	}
	return serviceAndResource[:serviceSep],
		serviceAndResource[serviceSep+1:],
		nil
}

func noFilter(msg http2stream.MsgItem) []http2stream.MsgItem {
	return []http2stream.MsgItem{msg}
}

func expectHeaders(headers map[string][]string, expect ...string) error {
	for _, expectName := range expect {
		entry, ok := headers[expectName]
		if !ok || len(entry) == 0 {
			return fmt.Errorf("Header %s is missing", expectName)
		}
	}
	return nil
}
