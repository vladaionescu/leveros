// Copyright 2013-2016 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"time"
)

// Task interface defines methods for asynchronous tasks.
type Task interface {
	IsDone() (bool, error)

	onComplete(ifc Task) chan error
	OnComplete() chan error
}

// BaseTask is used to poll for server task completion.
type BaseTask struct {
	cluster        *Cluster
	done           bool
	onCompleteChan chan error
}

// NewTask initializes task with fields needed to query server nodes.
func NewTask(cluster *Cluster, done bool) *BaseTask {
	return &BaseTask{
		cluster: cluster,
		done:    done,
	}
}

// Wait for asynchronous task to complete using default sleep interval.
func (btsk *BaseTask) onComplete(ifc Task) chan error {
	// create the channel if it doesn't exist yet
	if btsk.onCompleteChan != nil {
		// channel and goroutine already exists; just return the channel
		return btsk.onCompleteChan
	}

	btsk.onCompleteChan = make(chan error)

	// goroutine will loop every <interval> until IsDone() returns true or error
	const interval = 1 * time.Second
	go func() {
		// always close the channel on return
		defer close(btsk.onCompleteChan)

		for {
			select {
			case <-time.After(interval):
				done, err := ifc.IsDone()
				if err != nil {
					btsk.onCompleteChan <- err
					return
				} else if done {
					btsk.onCompleteChan <- nil
					return
				}
			} // select
		} // for
	}()

	return btsk.onCompleteChan
}
