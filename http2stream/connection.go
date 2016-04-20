package http2stream

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/leveros/leveros/leverutil"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

const (
	// http2MaxFrameLen specifies the max length of a HTTP2 frame.
	http2MaxFrameLen = 16384 // 16KiB frame
	// http2IOBufSize specifies the buffer size for sending frames.
	http2IOBufSize = 32 * 1024

	// The default value of flow control window size in HTTP2 spec.
	defaultWindowSize = 65535 // 64KiB
	// The initial window size for flow control.
	initialWindowSize     = defaultWindowSize      // for an RPC
	initialConnWindowSize = defaultWindowSize * 16 // for a connection

	contentType = "application/grpc"
)

var (
	connectionLogger = leverutil.GetLogger(PackageName, "connection")
)

type connectionState uint8

const (
	stateReachable = iota
	stateClosing
)

var (
	clientPreface = []byte(http2.ClientPreface)
)

type http2Connection struct {
	isServer bool

	// A handler function called whenever a new stream is started - server only.
	serverStreamHandler StreamHandler

	netConn       net.Conn
	writer        *bufio.Writer
	framer        *http2.Framer
	hpackBuffer   *bytes.Buffer
	hpackEncoder  *hpack.Encoder
	controlBuffer *leverutil.UnboundedChannel
	flowControl   *inFlow
	sendQuotaPool *quotaPool
	// writableChan is used as a mutex for the writing on the wire. In its
	// initial state (make(chan int, 1)) it is already acquired. Sending on it
	// releases it and receiving from it acquires it.
	// A channel is used rather than a sync.Mutex in order to listen to the
	// shutdownChan at the same time.
	writableChan chan int
	shutdownChan chan struct{}
	closedChan   chan struct{}
	maxStreamID  uint32

	// Guard the following.
	lock  sync.Mutex
	state connectionState
	// Per-stream outbound flow control window size set by peer.
	streamSendQuota uint32
	// The active streams.
	streams map[uint32]*HTTP2Stream // id -> stream
	// The next usable stream ID - client only.
	nextStreamID uint32
}

func newHTTP2Connection(
	isServer bool, netConn net.Conn,
	serverStreamHandler StreamHandler) (*http2Connection, error) {
	var hpackBuffer bytes.Buffer
	writer := bufio.NewWriterSize(netConn, http2IOBufSize)
	conn := &http2Connection{
		isServer:            isServer,
		serverStreamHandler: serverStreamHandler,
		netConn:             netConn,
		writer:              writer,
		framer:              http2.NewFramer(writer, netConn),
		hpackBuffer:         &hpackBuffer,
		hpackEncoder:        hpack.NewEncoder(&hpackBuffer),
		controlBuffer:       leverutil.NewUnboundedChannel(),
		flowControl:         &inFlow{limit: initialConnWindowSize},
		sendQuotaPool:       newQuotaPool(defaultWindowSize),
		writableChan:        make(chan int, 1),
		shutdownChan:        make(chan struct{}),
		closedChan:          make(chan struct{}),
		streamSendQuota:     defaultWindowSize,
		streams:             make(map[uint32]*HTTP2Stream),
	}

	if !isServer {
		conn.nextStreamID = 1 // Odd numbers for client-initiated streams.

		n, err := netConn.Write(clientPreface)
		if err != nil {
			return nil, err
		}
		if n != len(clientPreface) {
			return nil, fmt.Errorf("Preface length mismatch")
		}
	}

	var settings []http2.Setting
	if initialWindowSize != defaultWindowSize {
		settings = append(
			settings,
			http2.Setting{
				ID:  http2.SettingInitialWindowSize,
				Val: uint32(initialWindowSize),
			})
	}
	err := conn.framer.WriteSettings(settings...)
	if err != nil {
		return nil, err
	}
	windowDelta := uint32(initialConnWindowSize - defaultWindowSize)
	if windowDelta > 0 {
		err := conn.framer.WriteWindowUpdate(0, windowDelta)
		if err != nil {
			return nil, err
		}
	}
	conn.writer.Flush()
	conn.writableChan <- 0
	// The controller is responsible with writing frames on the wire.
	go conn.controller()
	// The reader reads in frames from the wire, handles stream 0 /
	// control frames and dispatches frames for the rest of the streams.
	go conn.reader()
	return conn, nil
}

func (conn *http2Connection) reader() {
	// Connection is considered closed when the reader goroutine finishes.
	defer close(conn.closedChan)

	if conn.isServer {
		// Check client preface.
		preface := make([]byte, len(clientPreface))
		_, err := io.ReadFull(conn.netConn, preface)
		if err != nil {
			conn.close(err)
			return
		}
		if !bytes.Equal(preface, clientPreface) {
			conn.close(fmt.Errorf("Unexpected client preface"))
			return
		}
	}

	// Read first frame (settings frame).
	frame, err := conn.framer.ReadFrame()
	if err != nil {
		conn.close(err)
		return
	}
	settingsFrame, ok := frame.(*http2.SettingsFrame)
	if !ok {
		conn.close(fmt.Errorf("Was expecting a settings frame"))
		return
	}
	conn.handleSettings(settingsFrame)

	// Keep reading frames.
	var curStream *HTTP2Stream
	hpackDec := newHPACKDecoder()
	var endStream bool
	for {
		frame, err = conn.framer.ReadFrame()
		if err != nil {
			if err != io.EOF {
				streamErr, isStreamErr := err.(http2.StreamError)
				if isStreamErr {
					conn.lock.Lock()
					stream, streamFound := conn.streams[streamErr.StreamID]
					conn.lock.Unlock()
					if streamFound {
						conn.closeStream(stream, err)
					}
				} else {
					connectionLogger.WithFields("err", err).Error("Read error")
					conn.close(err)
				}
			} else {
				conn.close(nil)
			}
			return
		}
		switch frame := frame.(type) {
		case *http2.HeadersFrame:
			if conn.isServer {
				curStream, err = conn.makeStream(frame.Header().StreamID)
				if err != nil {
					conn.close(err)
					return
				}
			} else {
				curStream, ok = conn.getStream(frame)
				if !ok {
					connectionLogger.WithFields(
						"streamID", frame.Header().StreamID,
					).Warning("Decoding header frame for closed stream")
					// Decoding needs to continue regardless in order to run
					// through hpack decoder. Otherwise state of decoder on the
					// next header frame will be messed up.
				}
			}
			endStream = frame.Header().Flags.Has(http2.FlagHeadersEndStream)
			endHeaders := conn.handleHeaders(
				hpackDec, curStream, frame, endStream)
			if endHeaders {
				curStream = nil
				endStream = false
				hpackDec.reset()
			}
		case *http2.ContinuationFrame:
			endHeaders := conn.handleHeaders(
				hpackDec, curStream, frame, endStream)
			if endHeaders {
				curStream = nil
				endStream = false
				hpackDec.reset()
			}
		case *http2.DataFrame:
			conn.handleData(frame)
		case *http2.RSTStreamFrame:
			stream, ok := conn.getStream(frame)
			if !ok {
				continue
			}
			conn.closeStream(stream, nil)
		case *http2.SettingsFrame:
			conn.handleSettings(frame)
		case *http2.PingFrame:
			pingAck := &ping{ack: true}
			copy(pingAck.data[:], frame.Data[:])
			conn.controlBuffer.Put(pingAck)
		case *http2.WindowUpdateFrame:
			id := frame.Header().StreamID
			incr := int(frame.Increment)
			if id == 0 {
				conn.sendQuotaPool.add(incr)
			} else {
				stream, ok := conn.getStream(frame)
				if !ok {
					continue
				}
				stream.sendQuotaPool.add(incr)
			}
		case *http2.GoAwayFrame:
			return
		default:
			connectionLogger.WithFields("frame", frame).Error(
				"Unhandled frame type")
		}
	}
}

func (conn *http2Connection) makeStream(id uint32) (*HTTP2Stream, error) {
	if conn.isServer {
		if id%2 != 1 || id <= conn.maxStreamID {
			return nil, fmt.Errorf(
				"Server received illegal stream ID %v when maxStreamID is %v",
				id, conn.maxStreamID)
		}
		conn.maxStreamID = id
	}
	flowControl := &inFlow{
		limit:      initialWindowSize,
		connInFlow: conn.flowControl,
	}
	stream := &HTTP2Stream{
		id:            id,
		conn:          conn,
		receiveBuffer: leverutil.NewUnboundedChannel(),
		flowControl:   flowControl,
		sendQuotaPool: newQuotaPool(int(conn.streamSendQuota)),
		shutdownChan:  make(chan struct{}),
	}
	return stream, nil
}

func (conn *http2Connection) newClientStream() (*HTTP2Stream, error) {
	conn.lock.Lock()
	if conn.state != stateReachable {
		conn.lock.Unlock()
		return nil, fmt.Errorf("Connection closing")
	}
	// Picking a stream ID will be done on the first write.
	// (Need writing exclusivity, to ensure stream IDs always grow.)
	stream, err := conn.makeStream(0)
	if err != nil {
		conn.lock.Unlock()
		return nil, err
	}
	conn.lock.Unlock()
	return stream, nil
}

func (conn *http2Connection) maybeInitStream(stream *HTTP2Stream) {
	// Note: This assumes that writing exclusivity is held.
	if conn.isServer {
		return
	}
	if stream.clientStreamInitialized {
		return
	}
	conn.lock.Lock()
	stream.id = conn.nextStreamID
	conn.nextStreamID += 2
	stream.clientStreamInitialized = true
	conn.streams[stream.id] = stream
	conn.lock.Unlock()
}

func (conn *http2Connection) write(stream *HTTP2Stream, item MsgItem) {
	switch item := item.(type) {
	case *MsgBytes:
		conn.writeBytes(stream, item.Data, item.EndStream)
		item.AfterRead()
	case *MsgEOF:
		conn.closeWriteStream(stream)
	case *MsgHeaders:
		conn.writeHeaders(stream, item.Headers, item.EndStream)
	case *MsgError:
		conn.closeStream(stream, item.Err)
	default:
		connectionLogger.WithFields("item", item).Panic("Invalid item")
	}
}

func (conn *http2Connection) writeBytes(
	stream *HTTP2Stream, data []byte, endStream bool) {
	dataBuffer := bytes.NewBuffer(data)
	needFinalFrame := endStream
	for needFinalFrame || dataBuffer.Len() > 0 {
		size := http2MaxFrameLen

		// Allocate quota for stream.
		stream.sendQuotaPool.add(0)
		streamQuota, err := conn.wait(stream.sendQuotaPool.acquire())
		if err != nil {
			conn.close(err)
			return
		}
		// Allocate quota for connection.
		conn.sendQuotaPool.add(0)
		connQuota, err := conn.wait(conn.sendQuotaPool.acquire())
		if err != nil {
			conn.close(err)
			return
		}
		if streamQuota < size {
			size = streamQuota
		}
		if connQuota < size {
			size = connQuota
		}
		chunk := dataBuffer.Next(size)
		chunkSize := len(chunk)
		if chunkSize < streamQuota {
			// Overbooked stream quota. Give it back.
			stream.sendQuotaPool.add(streamQuota - chunkSize)
		}
		if chunkSize < connQuota {
			// Overbooked connection quota. Give it back.
			conn.sendQuotaPool.add(connQuota - chunkSize)
		}

		// Acquire writing exclusivity.
		_, err = conn.wait(conn.writableChan)
		if err != nil {
			conn.controlBuffer.Put(&flushIO{})
			conn.close(err)
			return
		}
		conn.maybeInitStream(stream)
		endNow := endStream && dataBuffer.Len() == 0
		err = conn.framer.WriteData(stream.id, endNow, chunk)
		if err != nil {
			conn.close(err)
		}
		conn.writer.Flush()
		// Free writing rights.
		conn.writableChan <- 0

		needFinalFrame = false
	}

	if endStream {
		conn.closeWriteStream(stream)
	}
}

func (conn *http2Connection) writeHeaders(
	stream *HTTP2Stream, headers map[string][]string, endStream bool) {
	// Acquire writing exclusivity.
	_, err := conn.wait(conn.writableChan)
	if err != nil {
		conn.close(err)
		return
	}
	conn.maybeInitStream(stream)

	// Encode headers.
	conn.hpackBuffer.Reset()
	// First the pseudo (start with ':').
	for name, entry := range headers {
		if !strings.HasPrefix(name, ":") {
			continue
		}
		name = strings.ToLower(name)
		for _, value := range entry {
			conn.hpackEncoder.WriteField(hpack.HeaderField{
				Name:  name,
				Value: value,
			})
		}
	}
	// Then the regular (don't start with ':').
	for name, entry := range headers {
		if strings.HasPrefix(name, ":") {
			continue
		}
		name = strings.ToLower(name)
		for _, value := range entry {
			conn.hpackEncoder.WriteField(hpack.HeaderField{
				Name:  name,
				Value: value,
			})
		}
	}

	first := true
	endHeaders := false
	// Sends the headers in a single batch even when they span multiple frames.
	for !endHeaders {
		size := conn.hpackBuffer.Len()
		if size > http2MaxFrameLen {
			size = http2MaxFrameLen
		} else {
			endHeaders = true
		}
		if first {
			param := http2.HeadersFrameParam{
				StreamID:      stream.id,
				BlockFragment: conn.hpackBuffer.Next(size),
				EndStream:     endStream,
				EndHeaders:    endHeaders,
			}
			err = conn.framer.WriteHeaders(param)
			first = false
		} else {
			err = conn.framer.WriteContinuation(
				stream.id, endHeaders, conn.hpackBuffer.Next(size))
		}
		if err != nil {
			conn.writableChan <- 0
			conn.close(err)
			return
		}
	}
	if endStream {
		conn.writer.Flush()
	}

	// Free writing rights.
	conn.writableChan <- 0

	if endStream {
		conn.closeWriteStream(stream)
	}
}

func (conn *http2Connection) handleHeaders(
	hpackDec *hpackDecoder, stream *HTTP2Stream, frame headerFrame,
	endStream bool) (endHeaders bool) {
	endHeaders, err := hpackDec.decode(frame)
	if stream == nil {
		// Stream has been closed.
		return true
	}
	if err != nil {
		connectionLogger.WithFields("err", err).Error(
			"Error parsing headers")
		conn.controlBuffer.Put(&resetStream{stream.id, http2.ErrCodeInternal})
		return true
	}

	doClose := false
	if endStream {
		stream.lock.Lock()
		if stream.state != streamStateDone {
			if stream.state == streamStateWriteDone {
				doClose = true
			} else {
				stream.state = streamStateReadDone
			}
		}
		stream.lock.Unlock()
	}
	if !endHeaders {
		return false
	}
	// Received all header frames at this point.

	// Pass headers to stream object.
	headersCopy := make(map[string][]string)
	for key, entry := range hpackDec.headers {
		for _, value := range entry {
			headersCopy[key] = append(headersCopy[key], value)
		}
	}
	stream.receiveBuffer.Put(&MsgHeaders{
		Headers:   headersCopy,
		EndStream: endStream,
	})

	if conn.isServer {
		// Store headers in stream so that the user of the stream can know
		// what to do with the stream.
		if stream.serverHeaders == nil {
			stream.serverHeaders = hpackDec.headers
		}

		// Store newly created stream under http2 connection.
		conn.lock.Lock()
		if conn.state != stateReachable {
			conn.lock.Unlock()
			return true
		}
		conn.streams[stream.id] = stream
		conn.lock.Unlock()

		if conn.serverStreamHandler != nil {
			go conn.serverStreamHandler(stream)
		}
	}

	if doClose {
		conn.closeStream(stream, nil)
	}

	return true
}

func (conn *http2Connection) handleSettings(frame *http2.SettingsFrame) {
	if frame.IsAck() {
		return
	}
	var settingsList []http2.Setting
	frame.ForeachSetting(func(setting http2.Setting) error {
		settingsList = append(settingsList, setting)
		return nil
	})
	// The settings will be applied once the ack is sent.
	conn.controlBuffer.Put(&settings{
		ack:      true,
		settings: settingsList,
	})
}

func (conn *http2Connection) handleData(frame *http2.DataFrame) {
	stream, ok := conn.getStream(frame)
	if !ok {
		return
	}
	size := len(frame.Data())
	err := stream.flowControl.onData(uint32(size))
	if err != nil {
		conn.close(err)
		return
	}

	data := make([]byte, size)
	copy(data, frame.Data())
	endStream := frame.Header().Flags.Has(http2.FlagDataEndStream)

	doClose := false
	if endStream {
		stream.lock.Lock()
		if stream.state != streamStateDone {
			if stream.state == streamStateWriteDone {
				doClose = true
			} else {
				stream.state = streamStateReadDone
			}
		}
		stream.lock.Unlock()
	}
	afterRead := func() {
		conn.updateWindow(stream, uint32(size))
	}
	stream.receiveBuffer.Put(&MsgBytes{
		Data:      data,
		EndStream: endStream,
		AfterRead: afterRead,
	})

	if doClose {
		conn.closeStream(stream, nil)
	}
}

func (conn *http2Connection) applySettings(settings []http2.Setting) {
	for _, setting := range settings {
		if setting.ID == http2.SettingInitialWindowSize {
			conn.lock.Lock()
			for _, stream := range conn.streams {
				stream.sendQuotaPool.reset(
					int(setting.Val - conn.streamSendQuota))
			}
			conn.streamSendQuota = setting.Val
			conn.lock.Unlock()
		}
	}
}

func (conn *http2Connection) updateWindow(stream *HTTP2Stream, n uint32) {
	streamWindowUpdate, connWindowUpdate := stream.flowControl.onRead(n)
	if streamWindowUpdate > 0 {
		conn.controlBuffer.Put(
			&windowUpdate{stream.id, streamWindowUpdate})
	}
	if connWindowUpdate > 0 {
		conn.controlBuffer.Put(&windowUpdate{0, connWindowUpdate})
	}
}

func (conn *http2Connection) controller() {
	for {
		select {
		case controlItem := <-conn.controlBuffer.Get():
			conn.controlBuffer.Load()
			// Acquire writing rights.
			_, err := conn.wait(conn.writableChan)
			if err != nil {
				connectionLogger.WithFields("err", err).Error(
					"Error waiting to acquire write")
				return
			}
			switch controlItem := controlItem.(type) {
			case *windowUpdate:
				conn.framer.WriteWindowUpdate(
					controlItem.streamID, controlItem.increment)
				conn.writer.Flush()
			case *settings:
				if controlItem.ack {
					conn.framer.WriteSettingsAck()
					conn.writer.Flush()
					conn.applySettings(controlItem.settings)
				} else {
					conn.framer.WriteSettings(controlItem.settings...)
					conn.writer.Flush()
				}
			case *resetStream:
				conn.framer.WriteRSTStream(
					controlItem.streamID, controlItem.code)
				conn.writer.Flush()
			case *flushIO:
				conn.writer.Flush()
			case *ping:
				conn.framer.WritePing(
					controlItem.ack, controlItem.data)
				conn.writer.Flush()
			default:
				connectionLogger.WithFields("item", controlItem).Panic(
					"Invalid item")
			}
			// Free writing rights.
			conn.writableChan <- 0
		case <-conn.shutdownChan:
			return
		}
	}
}

func (conn *http2Connection) getStream(frame http2.Frame) (*HTTP2Stream, bool) {
	conn.lock.Lock()
	defer conn.lock.Unlock()
	if conn.streams == nil {
		// Connection is closing.
		return nil, false
	}
	stream, ok := conn.streams[frame.Header().StreamID]
	return stream, ok
}

func (conn *http2Connection) wait(proceed <-chan int) (int, error) {
	select {
	case <-conn.shutdownChan:
		return 0, fmt.Errorf("Closing")
	case i := <-proceed:
		return i, nil
	}
}

func (conn *http2Connection) closeWriteStream(stream *HTTP2Stream) {
	if conn.isServer {
		// For servers, closing write means closing the entire stream.
		// (To avoid leaving the resources of the stream at the mercy of the
		// client.)
		conn.closeStream(stream, nil)
		return
	}
	stream.lock.Lock()
	doClose := false
	if stream.state != streamStateDone {
		if stream.state == streamStateReadDone {
			doClose = true
		} else {
			stream.state = streamStateWriteDone
		}
	}
	stream.lock.Unlock()
	if doClose {
		conn.closeStream(stream, nil)
	}
}

func (conn *http2Connection) closeStream(stream *HTTP2Stream, err error) {
	conn.lock.Lock()
	if !conn.isServer && !stream.clientStreamInitialized {
		// Stream hasn't been used.
		conn.lock.Unlock()
		return
	}
	delete(conn.streams, stream.id)
	conn.lock.Unlock()
	quota := stream.flowControl.restoreConn()
	if quota > 0 {
		conn.controlBuffer.Put(&windowUpdate{0, quota})
	}

	stream.lock.Lock()
	if stream.state == streamStateDone {
		stream.lock.Unlock()
		return
	}
	stream.state = streamStateDone
	stream.lock.Unlock()

	if err != nil {
		conn.controlBuffer.Put(&resetStream{
			streamID: stream.id,
			code:     http2.ErrCodeInternal,
		})
		stream.receiveBuffer.Put(&MsgError{Err: err})
	} else {
		// TODO: This is not necessary in some cases
		//       (when EndStream already set).
		stream.receiveBuffer.Put(new(MsgEOF))
	}
	close(stream.shutdownChan)
}

func (conn *http2Connection) close(err error) {
	if err != nil {
		connectionLogger.WithFields("err", err).Error(
			"Connection closing due to error")
	}
	conn.lock.Lock()
	if conn.state == stateClosing {
		conn.lock.Unlock()
		return
	}
	conn.state = stateClosing
	streams := conn.streams
	conn.streams = nil
	conn.lock.Unlock()

	close(conn.shutdownChan)
	err = conn.netConn.Close()
	if err != nil {
		connectionLogger.WithFields("err", err).Error(
			"Error when closing connection")
	}

	for _, stream := range streams {
		conn.closeStream(stream, nil)
	}
}

func (conn *http2Connection) closed() <-chan struct{} {
	return conn.closedChan
}
