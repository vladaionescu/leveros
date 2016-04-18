package http2stream

import (
	"net"
	"sync"

	"github.com/leveros/leveros/leverutil"
)

var (
	serverLogger = leverutil.GetLogger(PackageName, "server")
)

// StreamHandler is a function that handles incoming streams.
type StreamHandler func(stream *HTTP2Stream)

// HTTP2Server represents a HTTP2 server which listens for connections.
type HTTP2Server struct {
	lock      sync.Mutex
	listeners map[net.Listener]bool
	conns     map[*http2Connection]bool
}

// NewHTTP2Server returns a new instance of HTTP2Server.
func NewHTTP2Server() *HTTP2Server {
	return &HTTP2Server{
		listeners: make(map[net.Listener]bool),
		conns:     make(map[*http2Connection]bool),
	}
}

// Serve listens for connections on provided address.
func (server *HTTP2Server) Serve(
	protocol string, listenAddr string,
	streamHandler StreamHandler) (net.Listener, chan struct{}, error) {
	stopServeChan := make(chan struct{})

	listener, err := net.Listen(protocol, listenAddr)
	if err != nil {
		close(stopServeChan)
		return nil, stopServeChan, err
	}

	server.lock.Lock()
	server.listeners[listener] = true
	server.lock.Unlock()
	go func() {
		defer func() {
			listener.Close()
			server.lock.Lock()
			defer server.lock.Unlock()
			delete(server.listeners, listener)
		}()
		for {
			netConn, err := listener.Accept()
			if err != nil {
				serverLogger.WithFields("err", err).Error(
					"Error accepting connections")
				close(stopServeChan)
				return
			}
			conn, err := newHTTP2Connection(true, netConn, streamHandler)
			if err != nil {
				serverLogger.WithFields("err", err).Error(
					"Error creating connection")
				continue
			}
			server.lock.Lock()
			server.conns[conn] = true
			server.lock.Unlock()
			go server.handleClose(conn)
		}
	}()
	return listener, stopServeChan, nil
}

func (server *HTTP2Server) handleClose(conn *http2Connection) {
	<-conn.closed()
	server.lock.Lock()
	delete(server.conns, conn)
	server.lock.Unlock()
}

// Stop causes the server to close all open connections and exit any serving
// loops.
func (server *HTTP2Server) Stop() {
	server.lock.Lock()
	for listener := range server.listeners {
		listener.Close()
	}
	for conn := range server.conns {
		conn.close(nil)
	}
	server.lock.Unlock()
}
