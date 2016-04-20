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
	"encoding/base64"
	"strings"

	. "github.com/aerospike/aerospike-client-go/types"
	. "github.com/aerospike/aerospike-client-go/types/atomic"
)

// Parse node partitions using new protocol. This is more code than a String.split() implementation,
// but it's faster because there are much fewer interim strings.
type partitionTokenizerNew struct {
	buffer []byte
	length int
	offset int
}

func newPartitionTokenizerNew(conn *Connection) (*partitionTokenizerNew, error) {
	pt := &partitionTokenizerNew{}

	// Use low-level info methods and parse byte array directly for maximum performance.
	// Send format:    replicas-master\n
	// Receive format: replicas-master\t<ns1>:<base 64 encoded bitmap>;<ns2>:<base 64 encoded bitmap>... \n
	infoMap, err := RequestInfo(conn, replicasName)
	if err != nil {
		return nil, err
	}

	info := infoMap[replicasName]
	pt.length = len(info)
	if pt.length == 0 {
		return nil, NewAerospikeError(PARSE_ERROR, replicasName+" is empty")
	}

	pt.buffer = []byte(info)

	return pt, nil
}

func (pt *partitionTokenizerNew) UpdatePartition(nmap map[string]*AtomicArray, node *Node) (map[string]*AtomicArray, error) {
	var amap map[string]*AtomicArray

	begin := pt.offset
	copied := false

	for pt.offset < pt.length {
		if pt.buffer[pt.offset] == ':' {
			// Parse namespace.
			namespace := strings.Trim(string(pt.buffer[begin:pt.offset]), " ")

			if len(namespace) <= 0 || len(namespace) >= 32 {
				response := pt.getTruncatedResponse()
				return nil, NewAerospikeError(PARSE_ERROR, "Invalid partition namespace "+
					namespace+". Response="+response)
			}

			pt.offset++
			begin = pt.offset

			// Parse partition id.
			for pt.offset < pt.length {
				b := pt.buffer[pt.offset]

				if b == ';' || b == '\n' {
					break
				}
				pt.offset++
			}

			if pt.offset == begin {
				response := pt.getTruncatedResponse()

				return nil, NewAerospikeError(PARSE_ERROR, "Empty partition id for namespace "+
					namespace+". Response="+response)
			}

			nodeArray, exists := nmap[namespace]

			if !exists {
				if !copied {
					// Make shallow copy of map.
					amap = make(map[string]*AtomicArray, len(nmap))
					for k, v := range nmap {
						amap[k] = v
					}
					copied = true
				}

				nodeArray = NewAtomicArray(_PARTITIONS)

				amap[namespace] = nodeArray
			}

			bitMapLength := pt.offset - begin
			restoreBuffer, err := base64.StdEncoding.DecodeString(string(pt.buffer[begin : begin+bitMapLength]))
			if err != nil {
				return nil, err
			}

			for i := 0; i < _PARTITIONS; i++ {
				if (restoreBuffer[i>>3] & (0x80 >> uint((i & 7)))) != 0 {
					// Logger.Info("Map: `" + namespace + "`," + strconv.Itoa(i) + "," + node.String())

					nodeArray.Set(i, node)
				}
			}
			pt.offset++
			begin = pt.offset
		} else {
			pt.offset++
		}
	}

	if copied {
		return amap, nil
	}
	return nil, nil
}

func (pt *partitionTokenizerNew) getTruncatedResponse() string {
	max := pt.length
	if pt.length > 200 {
		pt.length = max
	}
	return string(pt.buffer[:max])
}
