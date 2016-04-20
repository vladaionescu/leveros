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
	"bytes"
	"fmt"
	"math"
	"reflect"
	"time"

	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type packer struct {
	buffer *bytes.Buffer
	offset int
}

func packValueArray(val []Value) ([]byte, error) {
	packer := newPacker()
	if err := packer.packValueArray(val); err != nil {
		return nil, nil
	}
	return packer.buffer.Bytes(), nil
}

func packAnyArray(val []interface{}) ([]byte, error) {
	packer := newPacker()
	if err := packer.PackList(val); err != nil {
		return nil, nil
	}
	return packer.buffer.Bytes(), nil
}

func packAnyMap(val map[interface{}]interface{}) ([]byte, error) {
	packer := newPacker()
	if err := packer.PackMap(val); err != nil {
		return nil, nil
	}
	return packer.buffer.Bytes(), nil
}

///////////////////////////////////////////////////////////////////////////////

func newPacker() *packer {
	p := &packer{
		buffer: bytes.NewBuffer(make([]byte, 0, 256)),
	}

	return p
}

func (pckr *packer) packValueArray(values []Value) error {
	pckr.PackArrayBegin(len(values))
	for i := range values {
		if err := values[i].pack(pckr); err != nil {
			return err
		}
	}
	return nil
}

func (pckr *packer) PackList(list []interface{}) error {
	pckr.PackArrayBegin(len(list))
	for i := range list {
		if err := pckr.PackObject(list[i]); err != nil {
			return err
		}
	}
	return nil
}

func (pckr *packer) PackArrayBegin(size int) {
	if size < 16 {
		pckr.PackAByte(0x90 | byte(size))
	} else if size <= math.MaxUint16 {
		pckr.PackShort(0xdc, int16(size))
	} else {
		pckr.PackInt(0xdd, int32(size))
	}
}

func (pckr *packer) PackMap(theMap map[interface{}]interface{}) error {
	pckr.PackMapBegin(len(theMap))
	for k, v := range theMap {
		if k != nil {
			t := reflect.TypeOf(k)
			if t.Kind() == reflect.Map || t.Kind() == reflect.Slice ||
				(t.Kind() == reflect.Array && t.Elem().Kind() != reflect.Uint8) {
				panic("Maps, Slices, and bounded arrays other than Bounded Byte Arrays are not supported as Map keys.")
			}
		}

		if err := pckr.PackObject(k); err != nil {
			return err
		}
		if err := pckr.PackObject(v); err != nil {
			return err
		}
	}
	return nil
}

func (pckr *packer) PackMapBegin(size int) {
	if size < 16 {
		pckr.PackAByte(0x80 | byte(size))
	} else if size <= math.MaxUint16 {
		pckr.PackShort(0xde, int16(size))
	} else {
		pckr.PackInt(0xdf, int32(size))
	}
}

func (pckr *packer) PackBytes(b []byte) {
	pckr.PackByteArrayBegin(len(b) + 1)
	pckr.PackAByte(ParticleType.BLOB)
	pckr.PackByteArray(b, 0, len(b))
}

func (pckr *packer) PackByteArrayBegin(length int) {
	if length < 32 {
		pckr.PackAByte(0xa0 | byte(length))
	} else if length < 65536 {
		pckr.PackShort(0xda, int16(length))
	} else {
		pckr.PackInt(0xdb, int32(length))
	}
}

func (pckr *packer) PackObject(obj interface{}) error {
	switch v := obj.(type) {
	case Value:
		return v.pack(pckr)
	case string:
		pckr.PackString(v)
		return nil
	case []byte:
		pckr.PackBytes(obj.([]byte))
		return nil
	case int8:
		pckr.PackAInt(int(v))
		return nil
	case uint8:
		pckr.PackAInt(int(v))
		return nil
	case int16:
		pckr.PackAInt(int(v))
		return nil
	case uint16:
		pckr.PackAInt(int(v))
		return nil
	case int32:
		pckr.PackAInt(int(v))
		return nil
	case uint32:
		pckr.PackAInt(int(v))
		return nil
	case int:
		if Buffer.Arch32Bits {
			pckr.PackAInt(v)
			return nil
		}
		pckr.PackALong(int64(v))
		return nil
	case uint:
		if Buffer.Arch32Bits {
			pckr.PackAInt(int(v))
			return nil
		}
		pckr.PackAULong(uint64(v))
	case int64:
		pckr.PackALong(v)
		return nil
	case uint64:
		pckr.PackAULong(v)
		return nil
	case time.Time:
		pckr.PackALong(v.UnixNano())
		return nil
	case nil:
		pckr.PackNil()
		return nil
	case bool:
		pckr.PackBool(v)
		return nil
	case float32:
		pckr.PackFloat32(v)
		return nil
	case float64:
		pckr.PackFloat64(v)
		return nil
	case struct{}:
		pckr.PackMap(map[interface{}]interface{}{})
		return nil
	case []interface{}:
		return pckr.PackList(obj.([]interface{}))
	case map[interface{}]interface{}:
		return pckr.PackMap(obj.(map[interface{}]interface{}))
	}

	// check for array and map
	rv := reflect.ValueOf(obj)
	switch reflect.TypeOf(obj).Kind() {
	case reflect.Array, reflect.Slice:
		// pack bounded array of bytes differently
		if reflect.TypeOf(obj).Kind() == reflect.Array && reflect.TypeOf(obj).Elem().Kind() == reflect.Uint8 {
			l := rv.Len()
			arr := make([]byte, l)
			for i := 0; i < l; i++ {
				arr[i] = rv.Index(i).Interface().(uint8)
			}
			pckr.PackBytes(arr)
			return nil
		}

		l := rv.Len()
		arr := make([]interface{}, l)
		for i := 0; i < l; i++ {
			arr[i] = rv.Index(i).Interface()
		}
		return pckr.PackList(arr)
	case reflect.Map:
		l := rv.Len()
		amap := make(map[interface{}]interface{}, l)
		for _, i := range rv.MapKeys() {
			amap[i.Interface()] = rv.MapIndex(i).Interface()
		}
		return pckr.PackMap(amap)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return pckr.PackObject(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return pckr.PackObject(rv.Uint())
	case reflect.Bool:
		return pckr.PackObject(rv.Bool())
	case reflect.String:
		return pckr.PackObject(rv.String())
	case reflect.Float32, reflect.Float64:
		return pckr.PackObject(rv.Float())
	}

	panic(fmt.Sprintf("Type `%v` not supported to pack.", reflect.TypeOf(obj)))
}

func (pckr *packer) PackAULong(val uint64) {
	pckr.PackULong(val)
}

func (pckr *packer) PackALong(val int64) {
	if val >= 0 {
		if val < 128 {
			pckr.PackAByte(byte(val))
			return
		}

		if val <= math.MaxUint8 {
			pckr.PackByte(0xcc, byte(val))
			return
		}

		if val <= math.MaxUint16 {
			pckr.PackShort(0xcd, int16(val))
			return
		}

		if val <= math.MaxUint32 {
			pckr.PackInt(0xce, int32(val))
			return
		}
		pckr.PackLong(0xd3, val)
	} else {
		if val >= -32 {
			pckr.PackAByte(0xe0 | (byte(val) + 32))
			return
		}

		if val >= math.MinInt8 {
			pckr.PackByte(0xd0, byte(val))
			return
		}

		if val >= math.MinInt16 {
			pckr.PackShort(0xd1, int16(val))
			return
		}

		if val >= math.MinInt32 {
			pckr.PackInt(0xd2, int32(val))
			return
		}
		pckr.PackLong(0xd3, val)
	}
}

func (pckr *packer) PackAInt(val int) {
	if val >= 0 {
		if val < 128 {
			pckr.PackAByte(byte(val))
			return
		}

		if val < 256 {
			pckr.PackByte(0xcc, byte(val))
			return
		}

		if val < 65536 {
			pckr.PackShort(0xcd, int16(val))
			return
		}
		pckr.PackInt(0xce, int32(val))
	} else {
		if val >= -32 {
			pckr.PackAByte(0xe0 | (byte(val) + 32))
			return
		}

		if val >= math.MinInt8 {
			pckr.PackByte(0xd0, byte(val))
			return
		}

		if val >= math.MinInt16 {
			pckr.PackShort(0xd1, int16(val))
			return
		}
		pckr.PackInt(0xd2, int32(val))
	}
}

var _b8 = []byte{0, 0, 0, 0, 0, 0, 0, 0}
var _b4 = []byte{0, 0, 0, 0}
var _b2 = []byte{0, 0}

func (pckr *packer) grow(b []byte) int {
	pos := pckr.buffer.Len()
	pckr.buffer.Write(b)
	return pos
}

func (pckr *packer) PackString(val string) {
	size := len(val) + 1
	pckr.PackByteArrayBegin(size)
	pckr.buffer.WriteByte(byte(ParticleType.STRING))
	pckr.buffer.WriteString(val)
}

func (pckr *packer) PackGeoJson(val string) {
	size := len(val) + 1
	pckr.PackByteArrayBegin(size)
	pckr.buffer.WriteByte(byte(ParticleType.GEOJSON))
	pckr.buffer.WriteString(val)
}

func (pckr *packer) PackByteArray(src []byte, srcOffset int, srcLength int) {
	pckr.buffer.Write(src[srcOffset : srcOffset+srcLength])
}

func (pckr *packer) PackLong(valType int, val int64) {
	pckr.buffer.WriteByte(byte(valType))
	pos := pckr.grow(_b8)
	Buffer.Int64ToBytes(val, pckr.buffer.Bytes(), pos)
}

func (pckr *packer) PackULong(val uint64) {
	pckr.buffer.WriteByte(byte(0xcf))
	pos := pckr.grow(_b8)
	Buffer.Int64ToBytes(int64(val), pckr.buffer.Bytes(), pos)
}

func (pckr *packer) PackInt(valType int, val int32) {
	pckr.buffer.WriteByte(byte(valType))
	pos := pckr.grow(_b4)
	Buffer.Int32ToBytes(val, pckr.buffer.Bytes(), pos)
}

func (pckr *packer) PackShort(valType int, val int16) {
	pckr.buffer.WriteByte(byte(valType))
	pos := pckr.grow(_b2)
	Buffer.Int16ToBytes(val, pckr.buffer.Bytes(), pos)
}

// This method is not compatible with MsgPack specs and is only used by aerospike client<->server
// for wire transfer only
func (pckr *packer) PackShortRaw(val int16) {
	pos := pckr.grow(_b2)
	Buffer.Int16ToBytes(val, pckr.buffer.Bytes(), pos)
}

func (pckr *packer) PackByte(valType int, val byte) {
	pckr.buffer.WriteByte(byte(valType))
	pckr.buffer.WriteByte(val)
}

func (pckr *packer) PackNil() {
	pckr.buffer.WriteByte(0xc0)
}

func (pckr *packer) PackBool(val bool) {
	if val {
		pckr.buffer.WriteByte(0xc3)
	} else {
		pckr.buffer.WriteByte(0xc2)
	}
}

func (pckr *packer) PackFloat32(val float32) {
	pckr.buffer.WriteByte(0xca)
	pos := pckr.grow(_b4)
	Buffer.Float32ToBytes(val, pckr.buffer.Bytes(), pos)
}

func (pckr *packer) PackFloat64(val float64) {
	pckr.buffer.WriteByte(0xcb)
	pos := pckr.grow(_b8)
	Buffer.Float64ToBytes(val, pckr.buffer.Bytes(), pos)
}

func (pckr *packer) PackAByte(val byte) {
	pckr.buffer.WriteByte(val)
}
