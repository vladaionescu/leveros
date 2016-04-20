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

// ScanPolicy encapsulates parameters used in scan operations.
type ScanPolicy struct {
	*MultiPolicy

	// ScanPercent determines percent of data to scan.
	// Valid integer range is 1 to 100.
	// Default is 100.
	ScanPercent int //= 100;

	// ConcurrentNodes determines how to issue scan requests (in parallel or sequentially).
	ConcurrentNodes bool //= true;

	// Indicates if bin data is retrieved. If false, only record digests are retrieved.
	IncludeBinData bool //= true;

	// Include large data type bin values in addition to large data type bin names.
	// If false, LDT bin names will be returned, but LDT bin values will be empty.
	// If true,  LDT bin names and the entire LDT bin values will be returned.
	// Warning: LDT values may consume huge of amounts of memory depending on LDT size.
	IncludeLDT bool

	// FailOnClusterChange determines scan termination if cluster is in fluctuating state.
	FailOnClusterChange bool
}

// NewScanPolicy creates a new ScanPolicy instance with default values.
func NewScanPolicy() *ScanPolicy {
	res := &ScanPolicy{
		MultiPolicy:         NewMultiPolicy(),
		ScanPercent:         100,
		ConcurrentNodes:     true,
		IncludeBinData:      true,
		IncludeLDT:          false,
		FailOnClusterChange: true,
	}
	// Retry policy must be one-shot for scans.
	res.MaxRetries = 0

	return res
}
