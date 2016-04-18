package http2stream

import (
	"net"
	"time"

	"github.com/leveros/leveros/leverutil"
)

var clientLogger = leverutil.GetLogger(PackageName, "client")

// HTTP2Client represents a collection of client HTTP2 connections.
// The structure manages a connection pool internally.
type HTTP2Client struct {
	conns *leverutil.Cache
}

// NewHTTP2Client creates a new HTTP2Client.
func NewHTTP2Client(
	connectionConnectTimeout time.Duration,
	connectionExpiry time.Duration) (*HTTP2Client, error) {
	client := new(HTTP2Client)
	client.conns = leverutil.NewCache(
		connectionExpiry,
		func(addr string) (interface{}, error) {
			clientLogger.WithFields("addr", addr).Debug("New connection")
			netConn, err := net.DialTimeout(
				"tcp", addr, connectionConnectTimeout)
			if err != nil {
				return nil, err
			}
			conn, err := newHTTP2Connection(false, netConn, nil)
			if err != nil {
				return nil, err
			}
			// Remove connection from pool if it gets closed.
			go client.destroyOnClose(addr, conn)
			return conn, nil
		},
		func(element interface{}) {
			clientLogger.Debug("Closing connection")
			element.(*http2Connection).close(nil)
		})
	return client, nil
}

// NewStream creates a new HTTP2 stream to the provided server address.
// Also creates the underlying HTTP2 connection if it doesn't already exist.
func (client *HTTP2Client) NewStream(addr string) (*HTTP2Stream, error) {
	conn, err := client.conns.Get(addr)
	if err != nil {
		return nil, err
	}
	stream, err := conn.(*http2Connection).newClientStream()
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// KeepAlive resets the connection to addr's expiry.
func (client *HTTP2Client) KeepAlive(addr string) {
	client.conns.KeepAlive(addr)
}

func (client *HTTP2Client) destroyOnClose(addr string, conn *http2Connection) {
	<-conn.closed()
	client.conns.Destroy(addr)
}
