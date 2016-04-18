package http2stream

import (
	"sync"

	"github.com/leveros/leveros/leverutil"
)

type streamState uint8

const (
	streamStateActive    streamState = iota
	streamStateWriteDone             // EndStream sent
	streamStateReadDone              // EndStream received
	streamStateDone
)

//
// Messages that can be sent / received via a HTTP2Stream.

// MsgItem is a message sent / received from a stream (bytes, headers,
// errors or EOF).
type MsgItem interface {
	isMsgItem() bool
}

// MsgBytes represents a chunk of data sent / received by a HTTP2Stream on the
// wire.
type MsgBytes struct {
	Data      []byte
	EndStream bool
	// AfterRead needs to be called after the data has been consumed, to signal
	// the peer that it's ok to send more (flow control update window).
	AfterRead func()
}

func (*MsgBytes) isMsgItem() bool {
	return true
}

// MsgEOF represents the end data sent / received by a HTTP2Stream on the
// wire.
type MsgEOF struct {
}

func (*MsgEOF) isMsgItem() bool {
	return true
}

// MsgError represents a transport error of a HTTP2Stream.
type MsgError struct {
	Err error
}

func (*MsgError) isMsgItem() bool {
	return true
}

// MsgHeaders represents headers sent / received in a HTTP2Stream.
// Note: This may appear multiple times on a stream.
type MsgHeaders struct {
	Headers   map[string][]string
	EndStream bool
}

func (*MsgHeaders) isMsgItem() bool {
	return true
}

// FilterFun is a function that modifies a message before proxying it.
type FilterFun func(MsgItem) []MsgItem

// HTTP2Stream represents a HTTP2Stream connected to an endpoint (either client
// or server).
type HTTP2Stream struct {
	id            uint32
	conn          *http2Connection
	receiveBuffer *leverutil.UnboundedChannel
	flowControl   *inFlow
	sendQuotaPool *quotaPool
	shutdownChan  chan struct{}
	// The headers initally received by a server.
	serverHeaders map[string][]string

	// This is guarded by the connection lock.
	clientStreamInitialized bool

	lock  sync.Mutex
	state streamState
}

// GetHeaders returns the headers received from the client. These are available
// only to server streams.
func (stream *HTTP2Stream) GetHeaders() map[string][]string {
	return stream.serverHeaders
}

// GetReceiveBuffer returns the buffer of MsgItem's that can be consumed by a
// user application to handle messages from the stream.
func (stream *HTTP2Stream) GetReceiveBuffer() *leverutil.UnboundedChannel {
	return stream.receiveBuffer
}

// Write can be used to send messages on the wire.
func (stream *HTTP2Stream) Write(item MsgItem) {
	stream.conn.write(stream, item)
}

// ProxyTo causes the current stream to be proxied to provided destStream.
func (stream *HTTP2Stream) ProxyTo(
	destStream *HTTP2Stream, filterTo FilterFun, filterFrom FilterFun) {
	go stream.proxyLoop(destStream, filterTo)
	go destStream.proxyLoop(stream, filterFrom)
}

// Closed returns a channel that is closed when the stream is closed.
func (stream *HTTP2Stream) Closed() <-chan struct{} {
	return stream.shutdownChan
}

func (stream *HTTP2Stream) proxyLoop(other *HTTP2Stream, filter FilterFun) {
	buffer := stream.GetReceiveBuffer()
	for {
		select {
		case item := <-buffer.Get():
			buffer.Load()
			for _, filtered := range filter(item.(MsgItem)) {
				other.Write(filtered)
			}
		case <-stream.shutdownChan:
			// Consume any remaining messages.
			for {
				select {
				case item := <-buffer.Get():
					buffer.Load()
					for _, filtered := range filter(item.(MsgItem)) {
						other.Write(filtered)
					}
				default:
					return
				}
			}
		}
	}
}
