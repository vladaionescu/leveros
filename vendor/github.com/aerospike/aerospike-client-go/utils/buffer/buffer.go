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

package buffer

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	SizeOfInt32 = uintptr(4)
	SizeOfInt64 = uintptr(8)

	uint64sz = int(8)
	uint32sz = int(4)
	uint16sz = int(2)

	float32sz = int(4)
	float64sz = int(8)
)

var SizeOfInt uintptr

var Arch64Bits bool
var Arch32Bits bool

func init() {
	if 0 == ^uint(0xffffffff) {
		SizeOfInt = 4
	} else {
		SizeOfInt = 8
	}
	Arch64Bits = (SizeOfInt == SizeOfInt64)
	Arch32Bits = (SizeOfInt == SizeOfInt32)
}

// Coverts a byte slice into a hex string
func BytesToHexString(buf []byte) string {
	hlist := make([]byte, 3*len(buf))

	for i := range buf {
		hex := fmt.Sprintf("%02x ", buf[i])
		idx := i * 3
		copy(hlist[idx:], hex)
	}
	return string(hlist)
}

// converts a slice of bytes into an interger with appropriate type
func BytesToNumber(buf []byte, offset, length int) interface{} {
	if len(buf) == 0 {
		return int(0)
	}

	// This will work for negative integers too which
	// will be represented in two's compliment representation.
	val := int64(0)

	for i := 0; i < length; i++ {
		val <<= 8
		val = val | int64((buf[offset+i] & 0xFF))
	}

	if (SizeOfInt == SizeOfInt64) || (val <= math.MaxInt32 && val >= math.MinInt32) {
		return int(val)
	}

	return int64(val)
}

// Covertes a slice into int32; only maximum of 4 bytes will be used
func LittleBytesToInt32(buf []byte, offset int) int32 {
	l := len(buf[offset:])
	if l > uint32sz {
		l = uint32sz
	}
	r := int32(binary.LittleEndian.Uint32(buf[offset : offset+l]))
	return r
}

// Covertes a slice into int64; only maximum of 8 bytes will be used
func BytesToInt64(buf []byte, offset int) int64 {
	l := len(buf[offset:])
	if l > uint64sz {
		l = uint64sz
	}
	r := int64(binary.BigEndian.Uint64(buf[offset : offset+l]))
	return r
}

func VarBytesToInt64(buf []byte, offset int, len int) int64 {
	val := int64(0)
	for i := 0; i < len; i++ {
		val <<= 8
		val |= int64(buf[offset+i] & 0xFF)
	}
	return val
}

// Converts an int64 into slice of Bytes.
func Int64ToBytes(num int64, buffer []byte, offset int) []byte {
	if buffer != nil {
		binary.BigEndian.PutUint64(buffer[offset:], uint64(num))
		return nil
	}
	b := make([]byte, uint64sz)
	binary.BigEndian.PutUint64(b, uint64(num))
	return b
}

// Covertes a slice into int32; only maximum of 4 bytes will be used
func BytesToInt32(buf []byte, offset int) int32 {
	return int32(binary.BigEndian.Uint32(buf[offset : offset+uint32sz]))
}

// Covertes a slice into int32; only maximum of 4 bytes will be used
func BytesToUint32(buf []byte, offset int) uint32 {
	return binary.BigEndian.Uint32(buf[offset : offset+uint32sz])
}

// Converts an int32 to a byte slice of size 4
func Int32ToBytes(num int32, buffer []byte, offset int) []byte {
	if buffer != nil {
		binary.BigEndian.PutUint32(buffer[offset:], uint32(num))
		return nil
	}
	b := make([]byte, uint32sz)
	binary.BigEndian.PutUint32(b, uint32(num))
	return b
}

// Converts an int32 to a byte slice of size 4
func Uint32ToBytes(num uint32, buffer []byte, offset int) []byte {
	if buffer != nil {
		binary.BigEndian.PutUint32(buffer[offset:], num)
		return nil
	}
	b := make([]byte, uint32sz)
	binary.BigEndian.PutUint32(b, num)
	return b
}

// Converts
func BytesToInt16(buf []byte, offset int) int16 {
	return int16(binary.BigEndian.Uint16(buf[offset : offset+uint16sz]))
}

func BytesToUint16(buf []byte, offset int) uint16 {
	return binary.BigEndian.Uint16(buf[offset : offset+uint16sz])
}

// converts a
func Int16ToBytes(num int16, buffer []byte, offset int) []byte {
	if buffer != nil {
		binary.BigEndian.PutUint16(buffer[offset:], uint16(num))
		return nil
	}
	b := make([]byte, uint16sz)
	binary.BigEndian.PutUint16(b, uint16(num))
	return b
}

func BytesToFloat32(buf []byte, offset int) float32 {
	bits := binary.BigEndian.Uint32(buf[offset : offset+float32sz])
	return math.Float32frombits(bits)
}

func Float32ToBytes(float float32, buffer []byte, offset int) []byte {
	bits := math.Float32bits(float)
	if buffer != nil {
		binary.BigEndian.PutUint32(buffer[offset:], bits)
		return nil
	}

	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, bits)
	return bytes
}

func BytesToFloat64(buf []byte, offset int) float64 {
	bits := binary.BigEndian.Uint64(buf[offset : offset+float64sz])
	return math.Float64frombits(bits)
}

func Float64ToBytes(float float64, buffer []byte, offset int) []byte {
	bits := math.Float64bits(float)
	if buffer != nil {
		binary.BigEndian.PutUint64(buffer[offset:], bits)
		return nil
	}

	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

func GetUnsigned(b byte) int {
	r := b

	if r < 0 {
		r = r & 0x7f
		r = r | 0x80
	}
	return int(r)
}
