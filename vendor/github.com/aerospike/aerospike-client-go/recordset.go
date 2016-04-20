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
	"fmt"
	"reflect"
	"sync"

	. "github.com/aerospike/aerospike-client-go/types/atomic"
)

type Result struct {
	Record *Record
	Err    error
}

// String implements the Stringer interface
func (res *Result) String() string {
	if res.Record != nil {
		return fmt.Sprintf("%v", res.Record)
	}
	return fmt.Sprintf("%v", res.Err)
}

// Objectset encapsulates the result of Scan and Query commands.
type objectset struct {
	// a reference to the object channel to close on end signal
	objChan reflect.Value

	// Errors is a channel on which all errors will be sent back.
	// NOTE: Do not use Errors directly. Range on channel returned by Results() instead.
	// This field is deprecated and will be unexported in the future
	Errors chan error

	wgGoroutines sync.WaitGroup
	goroutines   *AtomicInt

	active    *AtomicBool
	cancelled chan struct{}

	chanLock sync.Mutex
}

// Recordset encapsulates the result of Scan and Query commands.
type Recordset struct {
	objectset

	// Records is a channel on which the resulting records will be sent back.
	// NOTE: Do not use Records directly. Range on channel returned by Results() instead.
	// Will be unexported in the future
	Records chan *Record
}

// newObjectset generates a new RecordSet instance.
func newObjectset(objChan reflect.Value, goroutines int) *objectset {

	if objChan.Kind() != reflect.Chan ||
		objChan.Type().Elem().Kind() != reflect.Ptr ||
		objChan.Type().Elem().Elem().Kind() != reflect.Struct {
		panic("Scan/Query object channels should be of type `chan *T`")
	}

	rs := &objectset{
		objChan:    objChan,
		Errors:     make(chan error, goroutines),
		active:     NewAtomicBool(true),
		goroutines: NewAtomicInt(goroutines),
		cancelled:  make(chan struct{}),
	}
	rs.wgGoroutines.Add(goroutines)

	return rs
}

// newRecordset generates a new RecordSet instance.
func newRecordset(recSize, goroutines int) *Recordset {
	var nilChan chan *struct{}

	rs := &Recordset{
		Records:   make(chan *Record, recSize),
		objectset: *newObjectset(reflect.ValueOf(nilChan), goroutines),
	}

	return rs
}

// IsActive returns true if the operation hasn't been finished or cancelled.
func (rcs *Recordset) IsActive() bool {
	return rcs.active.Get()
}

// Results returns a new receive-only channel with the results of the Scan/Query.
// This is a more idiomatic approach to the iterator pattern in getting the
// results back from the recordset, and doesn't require the user to write the
// ugly select in their code.
// Result contains a Record and an error reference.
//
// Example:
//
//  recordset, err := client.ScanAll(nil, namespace, set)
//  handleError(err)
//  for res := range recordset.Results() {
//    if res.Err != nil {
//      // handle error here
//    } else {
//      // process record here
//      fmt.Println(res.Record.Bins)
//    }
//  }
func (rcs *Recordset) Results() <-chan *Result {
	res := make(chan *Result, len(rcs.Records))

	go func() {
		defer close(res)
		for {
			select {
			case r, ok := <-rcs.Records:
				if !ok {
					for e := range rcs.Errors {
						// empty Errors channel and/or wait until it's also closed
						res <- &Result{Record: nil, Err: e}
					}
					return
				}
				res <- &Result{Record: r, Err: nil}
			case e, ok := <-rcs.Errors:
				if !ok {
					for r := range rcs.Records {
						// empty Records channel and/or wait until it's also closed
						res <- &Result{Record: r, Err: nil}
					}
					return
				}
				res <- &Result{Record: nil, Err: e}
			}
		}
	}()

	return (<-chan *Result)(res)
}

// Close all streams from different nodes.
func (rcs *Recordset) Close() {
	// do it only once
	if rcs.active.CompareAndToggle(true) {
		// this will broadcast to all commands listening to the channel
		close(rcs.cancelled)

		// wait till all goroutines are done
		rcs.wgGoroutines.Wait()

		rcs.chanLock.Lock()
		defer rcs.chanLock.Unlock()

		if rcs.Records != nil {
			close(rcs.Records)
		} else if rcs.objChan.IsValid() {
			rcs.objChan.Close()
		}

		close(rcs.Errors)
	}
}

func (rcs *Recordset) signalEnd() {
	rcs.wgGoroutines.Done()
	if rcs.goroutines.DecrementAndGet() == 0 {
		rcs.Close()
	}
}

func (rcs *Recordset) sendError(err error) {
	rcs.chanLock.Lock()
	defer rcs.chanLock.Unlock()
	if rcs.IsActive() {
		rcs.Errors <- err
	}
}
