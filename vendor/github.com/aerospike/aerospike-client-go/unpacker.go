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

	. "github.com/aerospike/aerospike-client-go/types"
	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type unpacker struct {
	buffer []byte
	offset int
	length int
}

func newUnpacker(buffer []byte, offset int, length int) *unpacker {
	return &unpacker{
		buffer: buffer,
		offset: offset,
		length: length,
	}
}

func (upckr *unpacker) UnpackList() ([]interface{}, error) {
	if upckr.length <= 0 {
		return nil, nil
	}

	theType := upckr.buffer[upckr.offset] & 0xff
	upckr.offset++
	var count int

	if (theType & 0xf0) == 0x90 {
		count = int(theType & 0x0f)
	} else if theType == 0xdc {
		count = int(Buffer.BytesToUint16(upckr.buffer, upckr.offset))
		upckr.offset += 2
	} else if theType == 0xdd {
		count = int(Buffer.BytesToUint32(upckr.buffer, upckr.offset))
		upckr.offset += 4
	} else {
		return nil, nil
	}

	return upckr.unpackList(count)
}

func (upckr *unpacker) unpackList(count int) ([]interface{}, error) {
	out := make([]interface{}, 0, count)

	for i := 0; i < count; i++ {
		obj, err := upckr.unpackObject(false)
		if err != nil {
			return nil, err
		}
		out = append(out, obj)
	}
	return out, nil
}

func (upckr *unpacker) UnpackMap() (map[interface{}]interface{}, error) {
	if upckr.length <= 0 {
		return nil, nil
	}

	theType := upckr.buffer[upckr.offset] & 0xff
	upckr.offset++
	var count int

	if (theType & 0xf0) == 0x80 {
		count = int(theType & 0x0f)
	} else if theType == 0xde {
		count = int(Buffer.BytesToUint16(upckr.buffer, upckr.offset))
		upckr.offset += 2
	} else if theType == 0xdf {
		count = int(Buffer.BytesToUint32(upckr.buffer, upckr.offset))
		upckr.offset += 4
	} else {
		return make(map[interface{}]interface{}), nil
	}
	return upckr.unpackMap(count)
}

func (upckr *unpacker) unpackMap(count int) (map[interface{}]interface{}, error) {
	out := make(map[interface{}]interface{}, count)

	for i := 0; i < count; i++ {
		key, err := upckr.unpackObject(true)
		if err != nil {
			return nil, err
		}

		val, err := upckr.unpackObject(false)
		if err != nil {
			return nil, err
		}
		out[key] = val
	}
	return out, nil
}

func (upckr *unpacker) unpackObjects() (interface{}, error) {
	if upckr.length <= 0 {
		return nil, nil
	}

	return upckr.unpackObject(false)
}

func (upckr *unpacker) unpackBlob(count int, isMapKey bool) (interface{}, error) {
	theType := upckr.buffer[upckr.offset] & 0xff
	upckr.offset++
	count--
	var val interface{}

	switch theType {
	case ParticleType.STRING:
		val = string(upckr.buffer[upckr.offset : upckr.offset+count])

	case ParticleType.BLOB:
		if isMapKey {
			b := reflect.Indirect(reflect.New(reflect.ArrayOf(count, reflect.TypeOf(byte(0)))))
			reflect.Copy(b, reflect.ValueOf(upckr.buffer[upckr.offset:upckr.offset+count]))

			val = b.Interface()
		} else {
			b := make([]byte, count)
			copy(b, upckr.buffer[upckr.offset:upckr.offset+count])
			val = b
		}

	case ParticleType.GEOJSON:
		val = NewGeoJSONValue(string(upckr.buffer[upckr.offset : upckr.offset+count]))

	default:
		panic(NewAerospikeError(SERIALIZE_ERROR, fmt.Sprintf("Error while unpacking BLOB. Type-header with code `%d` not recognized.", theType)))
	}
	upckr.offset += count

	return val, nil
}

func (upckr *unpacker) unpackObject(isMapKey bool) (interface{}, error) {
	theType := upckr.buffer[upckr.offset] & 0xff
	upckr.offset++

	switch theType {
	case 0xc0:
		return nil, nil

	case 0xc3:
		return true, nil

	case 0xc2:
		return false, nil

	case 0xca:
		val := Buffer.BytesToFloat32(upckr.buffer, upckr.offset)
		upckr.offset += 4
		return val, nil

	case 0xcb:
		val := Buffer.BytesToFloat64(upckr.buffer, upckr.offset)
		upckr.offset += 8
		return val, nil

	case 0xcc:
		r := upckr.buffer[upckr.offset] & 0xff
		upckr.offset++

		return int(r), nil

	case 0xcd:
		val := uint16(Buffer.BytesToInt16(upckr.buffer, upckr.offset))
		upckr.offset += 2
		return int(val), nil

	case 0xce:
		val := uint32(Buffer.BytesToInt32(upckr.buffer, upckr.offset))
		upckr.offset += 4

		if Buffer.Arch64Bits {
			return int(val), nil
		}
		return int64(val), nil

	case 0xcf:
		val := Buffer.BytesToInt64(upckr.buffer, upckr.offset)
		upckr.offset += 8
		return uint64(val), nil

	case 0xd0:
		r := int8(upckr.buffer[upckr.offset])
		upckr.offset++
		return int(r), nil

	case 0xd1:
		val := Buffer.BytesToInt16(upckr.buffer, upckr.offset)
		upckr.offset += 2
		return int(val), nil

	case 0xd2:
		val := Buffer.BytesToInt32(upckr.buffer, upckr.offset)
		upckr.offset += 4
		return int(val), nil

	case 0xd3:
		val := Buffer.BytesToInt64(upckr.buffer, upckr.offset)
		upckr.offset += 8
		if Buffer.Arch64Bits {
			return int(val), nil
		}
		return int64(val), nil

	case 0xda:
		count := int(Buffer.BytesToUint16(upckr.buffer, upckr.offset))
		upckr.offset += 2
		return upckr.unpackBlob(count, isMapKey)

	case 0xdb:
		count := int(Buffer.BytesToUint32(upckr.buffer, upckr.offset))
		upckr.offset += 4
		return upckr.unpackBlob(count, isMapKey)

	case 0xdc:
		count := int(Buffer.BytesToUint16(upckr.buffer, upckr.offset))
		upckr.offset += 2
		return upckr.unpackList(count)

	case 0xdd:
		count := int(Buffer.BytesToUint32(upckr.buffer, upckr.offset))
		upckr.offset += 4
		return upckr.unpackList(count)

	case 0xde:
		count := int(Buffer.BytesToUint16(upckr.buffer, upckr.offset))
		upckr.offset += 2
		return upckr.unpackMap(count)

	case 0xdf:
		count := int(Buffer.BytesToUint32(upckr.buffer, upckr.offset))
		upckr.offset += 4
		return upckr.unpackMap(count)

	default:
		if (theType & 0xe0) == 0xa0 {
			return upckr.unpackBlob(int(theType&0x1f), isMapKey)
		}

		if (theType & 0xf0) == 0x80 {
			return upckr.unpackMap(int(theType & 0x0f))
		}

		if (theType & 0xf0) == 0x90 {
			count := int(theType & 0x0f)
			return upckr.unpackList(count)
		}

		if theType < 0x80 {
			return int(theType), nil
		}

		if theType >= 0xe0 {
			return int(int(theType) - 0xe0 - 32), nil
		}
	}

	return nil, NewAerospikeError(SERIALIZE_ERROR)
}
