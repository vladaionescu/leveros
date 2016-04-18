package http2stream

import (
	"fmt"
	"sync"

	"golang.org/x/net/http2"
)

//
// Control actions.

type windowUpdate struct {
	streamID  uint32
	increment uint32
}

type settings struct {
	ack      bool
	settings []http2.Setting
}

type resetStream struct {
	streamID uint32
	code     http2.ErrCode
}

type flushIO struct {
}

type ping struct {
	ack  bool
	data [8]byte
}

// inFlow manages inbound flow control.
type inFlow struct {
	// The inbound flow control limit for pending data.
	limit uint32

	// The connection's inFlow. Set to nil for stream 0.
	connInFlow *inFlow

	lock sync.Mutex
	// Overall data received but not consumed.
	pendingData uint32
	// Data consumed but we have not sent window update for yet.
	pendingUpdate uint32
}

// onData is invoked when some data frame is received. It increments not only its
// own pendingData but also that of the associated connection-level flow.
func (f *inFlow) onData(n uint32) error {
	if n == 0 {
		return nil
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.pendingData+f.pendingUpdate+n > f.limit {
		return fmt.Errorf("recieved %d-bytes data exceeding the limit %d bytes", f.pendingData+f.pendingUpdate+n, f.limit)
	}
	if f.connInFlow != nil {
		err := f.connInFlow.onData(n)
		if err != nil {
			return err
		}
	}
	f.pendingData += n
	return nil
}

// connOnRead updates the connection level states when the application consumes data.
func (f *inFlow) connOnRead(n uint32) uint32 {
	if n == 0 || f.connInFlow != nil {
		return 0
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.pendingData -= n
	f.pendingUpdate += n
	if f.pendingUpdate >= f.limit/4 {
		ret := f.pendingUpdate
		f.pendingUpdate = 0
		return ret
	}
	return 0
}

// onRead is invoked when the application reads the data. It returns the window updates
// for both stream and connection level.
func (f *inFlow) onRead(n uint32) (swu, cwu uint32) {
	if n == 0 {
		return
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.pendingData == 0 {
		// pendingData has been adjusted by restoreConn.
		return
	}
	f.pendingData -= n
	f.pendingUpdate += n
	if f.pendingUpdate >= f.limit/4 {
		swu = f.pendingUpdate
		f.pendingUpdate = 0
	}
	cwu = f.connInFlow.connOnRead(n)
	return
}

// restoreConn is invoked when a stream is terminated. It removes its stake in
// the connection-level flow and resets its own state.
func (f *inFlow) restoreConn() uint32 {
	if f.connInFlow == nil {
		return 0
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	n := f.pendingData
	f.pendingData = 0
	f.pendingUpdate = 0
	return f.connInFlow.connOnRead(n)
}

// quotaPool keeps track of window quota.
type quotaPool struct {
	acquireChannel chan int

	lock  sync.Mutex
	quota int
}

// newQuotaPool creates a quotaPool which has quota q available to consume.
func newQuotaPool(q int) *quotaPool {
	qp := &quotaPool{
		acquireChannel: make(chan int, 1),
	}
	if q > 0 {
		qp.acquireChannel <- q
	} else {
		qp.quota = q
	}
	return qp
}

// add adds n to the available quota and tries to send it on acquire.
func (qp *quotaPool) add(n int) {
	qp.lock.Lock()
	defer qp.lock.Unlock()
	qp.quota += n
	if qp.quota <= 0 {
		return
	}
	select {
	case qp.acquireChannel <- qp.quota:
		qp.quota = 0
	default:
	}
}

// cancel cancels the pending quota sent on acquire, if any.
func (qp *quotaPool) cancel() {
	qp.lock.Lock()
	defer qp.lock.Unlock()
	select {
	case n := <-qp.acquireChannel:
		qp.quota += n
	default:
	}
}

// reset cancels the pending quota sent on acquired, incremented by v and sends
// it back on acquire.
func (qp *quotaPool) reset(v int) {
	qp.lock.Lock()
	defer qp.lock.Unlock()
	select {
	case n := <-qp.acquireChannel:
		qp.quota += n
	default:
	}
	qp.quota += v
	if qp.quota <= 0 {
		return
	}
	select {
	case qp.acquireChannel <- qp.quota:
		qp.quota = 0
	default:
	}
}

// acquire returns the channel on which available quota amounts are sent.
func (qp *quotaPool) acquire() <-chan int {
	return qp.acquireChannel
}
