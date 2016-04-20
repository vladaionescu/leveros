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

import . "github.com/aerospike/aerospike-client-go/types"

// BinMap is used to define a map of bin names to values.
type BinMap map[string]interface{}

// Bin encapsulates a field name/value pair.
type Bin struct {
	// Bin name. Current limit is 14 characters.
	Name string

	// Bin value.
	Value Value
}

// NewBin generates a new Bin instance, specifying bin name and string value.
// For servers configured as "single-bin", enter an empty name.
func NewBin(name string, value interface{}) *Bin {
	return &Bin{
		Name:  name,
		Value: NewValue(value),
	}
}

// String implements Stringer interface.
func (bn *Bin) String() string {
	return bn.Name + ":" + bn.Value.String()
}

func binMapToBins(bins []*Bin, binMap BinMap) []*Bin {
	i := 0
	for k, v := range binMap {
		bins[i].Name = k
		bins[i].Value = NewValue(v)
		i++
	}

	return bins
}

// pool Bins so that we won't have to allocate them every time
var binPool = NewPool(512)

func init() {
	binPool.New = func(params ...interface{}) interface{} {
		size := params[0].(int)
		bins := make([]*Bin, size, size)
		for i := range bins {
			bins[i] = &Bin{}
		}
		return bins
	}

	binPool.IsUsable = func(obj interface{}, params ...interface{}) bool {
		return len(obj.([]*Bin)) >= params[0].(int)
	}
}
