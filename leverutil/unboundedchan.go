package leverutil

import (
	"sync"
)

// UnboundedChannel is a channel with unlimited capacity.
type UnboundedChannel struct {
	channel chan interface{}
	lock    sync.Mutex
	backlog []interface{}
}

// NewUnboundedChannel returns a new UnboundedChannel.
func NewUnboundedChannel() *UnboundedChannel {
	return &UnboundedChannel{
		channel: make(chan interface{}, 1),
	}
}

// Put appends the element to the channel.
func (uc *UnboundedChannel) Put(element interface{}) {
	uc.lock.Lock()
	defer uc.lock.Unlock()
	uc.backlog = append(uc.backlog, element)
	uc.loadInternal()
}

// Load loads the next item to be received via Get.
func (uc *UnboundedChannel) Load() {
	uc.lock.Lock()
	defer uc.lock.Unlock()
	uc.loadInternal()
}

func (uc *UnboundedChannel) loadInternal() {
	if len(uc.backlog) > 0 {
		select {
		case uc.channel <- uc.backlog[0]:
			uc.backlog = uc.backlog[1:]
		default:
		}
	}
}

// Get returns the channel that can be listened to for the next available
// element. Call Load right after receiving anything from the channel in order
// to load the next element for receiving.
func (uc *UnboundedChannel) Get() <-chan interface{} {
	return uc.channel
}
