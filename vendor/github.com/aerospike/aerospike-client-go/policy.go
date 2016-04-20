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

// Policy Interface
type Policy interface {
	// Retrives BasePolicy
	GetBasePolicy() *BasePolicy
}

// BasePolicy excapsulates parameters for transaction policy attributes
// used in all database operation calls.
type BasePolicy struct {
	Policy

	// Priority of request relative to other transactions.
	// Currently, only used for scans.
	Priority Priority //= Priority.DEFAULT;

	// How replicas should be consulted in a read operation to provide the desired
	// consistency guarantee. Default to allowing one replica to be used in the
	// read operation.
	ConsistencyLevel ConsistencyLevel //= CONSISTENCY_ONE

	// Timeout specifies transaction timeout.
	// This timeout is used to set the socket timeout and is also sent to the
	// server along with the transaction in the wire protocol.
	// Default to no timeout (0).
	Timeout time.Duration

	// MaxRetries determines maximum number of retries before aborting the current transaction.
	// A retry is attempted when there is a network error other than timeout.
	// If maxRetries is exceeded, the abort will occur even if the timeout
	// has not yet been exceeded.
	MaxRetries int //= 2;

	// SleepBetweenReplies determines duration to sleep between retries if a transaction fails and the
	// timeout was not exceeded.  Enter zero to skip sleep.
	SleepBetweenRetries time.Duration //= 500ms;
}

// NewPolicy generates a new BasePolicy instance with default values.
func NewPolicy() *BasePolicy {
	return &BasePolicy{
		Priority:            DEFAULT,
		ConsistencyLevel:    CONSISTENCY_ONE,
		Timeout:             0 * time.Millisecond,
		MaxRetries:          2,
		SleepBetweenRetries: 500 * time.Millisecond,
	}
}

var _ Policy = &BasePolicy{}

// GetBasePolicy returns embedded BasePolicy in all types that embed this struct.
func (p *BasePolicy) GetBasePolicy() *BasePolicy { return p }
