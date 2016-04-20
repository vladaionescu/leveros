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
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"

	// . "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

// Value interface is used to efficiently serialize objects into the wire protocol.
type Value interface {

	// Calculate number of vl.bytes necessary to serialize the value in the wire protocol.
	estimateSize() int

	// Serialize the value in the wire protocol.
	write(buffer []byte, offset int) (int, error)

	// Serialize the value using MessagePack.
	pack(packer *packer) error

	// GetType returns wire protocol value type.
	GetType() int

	// GetObject returns original value as an interface{}.
	GetObject() interface{}

	// String implements Stringer interface.
	String() string

	reader() io.Reader
}

// AerospikeBlob defines an interface to automatically
// encode an object to a []byte.
type AerospikeBlob interface {
	// EncodeBlob returns a byte slice representing the encoding of the
	// receiver for transmission to a Decoder, usually of the same
	// concrete type.
	EncodeBlob() ([]byte, error)
}

// NewValue generates a new Value object based on the type.
// If the type is not supported, NewValue will panic.
func NewValue(v interface{}) Value {
	switch val := v.(type) {
	case nil:
		return &NullValue{}
	case int:
		return NewIntegerValue(val)
	case int64:
		return NewLongValue(val)
	case string:
		return NewStringValue(val)
	case []Value:
		return NewValueArray(val)
	case []byte:
		return NewBytesValue(val)
	case int8:
		return NewIntegerValue(int(val))
	case int16:
		return NewIntegerValue(int(val))
	case int32:
		return NewIntegerValue(int(val))
	case uint8: // byte supported here
		return NewIntegerValue(int(val))
	case uint16:
		return NewIntegerValue(int(val))
	case uint32:
		return NewIntegerValue(int(val))
	case float32:
		return NewFloatValue(float64(val))
	case float64:
		return NewFloatValue(val)
	case uint:
		if !Buffer.Arch64Bits || (val <= math.MaxInt64) {
			return NewLongValue(int64(val))
		}
	case []interface{}:
		return NewListValue(val)
	case map[interface{}]interface{}:
		return NewMapValue(val)
	case Value:
		return val
	case AerospikeBlob:
		return NewBlobValue(val)
	}

	// check for array and map
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Array, reflect.Slice:
		l := rv.Len()
		arr := make([]interface{}, l)
		for i := 0; i < l; i++ {
			arr[i] = rv.Index(i).Interface()
		}

		return NewListValue(arr)
	case reflect.Map:
		l := rv.Len()
		amap := make(map[interface{}]interface{}, l)
		for _, i := range rv.MapKeys() {
			amap[i.Interface()] = rv.MapIndex(i).Interface()
		}

		return NewMapValue(amap)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return NewLongValue(reflect.ValueOf(v).Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return NewLongValue(int64(reflect.ValueOf(v).Uint()))
	case reflect.String:
		return NewStringValue(rv.String())
	}

	// panic for anything that is not supported.
	panic(NewAerospikeError(TYPE_NOT_SUPPORTED, "Value type '"+reflect.TypeOf(v).Name()+"' not supported"))
}

// NullValue is an empty value.
type NullValue struct{}

var nullValue NullValue

// NewNullValue generates a NullValue instance.
func NewNullValue() NullValue {
	return nullValue
}

func (vl NullValue) estimateSize() int {
	return 0
}

func (vl NullValue) write(buffer []byte, offset int) (int, error) {
	return 0, nil
}

func (vl NullValue) pack(packer *packer) error {
	packer.PackNil()
	return nil
}

// GetType returns wire protocol value type.
func (vl NullValue) GetType() int {
	return ParticleType.NULL
}

// GetObject returns original value as an interface{}.
func (vl NullValue) GetObject() interface{} {
	return nil
}

func (vl NullValue) reader() io.Reader {
	return nil
}

// String implements Stringer interface.
func (vl NullValue) String() string {
	return ""
}

///////////////////////////////////////////////////////////////////////////////

// BytesValue encapsulates an array of bytes.
type BytesValue []byte

// NewBytesValue generates a ByteValue instance.
func NewBytesValue(bytes []byte) BytesValue {
	return BytesValue(bytes)
}

// NewBlobValue accepts an AerospikeBlob interface, and automatically
// converts it to a BytesValue.
// If Encode returns an err, it will panic.
func NewBlobValue(object AerospikeBlob) BytesValue {
	buf, err := object.EncodeBlob()
	if err != nil {
		panic(err)
	}

	return NewBytesValue(buf)
}

func (vl BytesValue) estimateSize() int {
	return len(vl)
}

func (vl BytesValue) write(buffer []byte, offset int) (int, error) {
	len := copy(buffer[offset:], vl)
	return len, nil
}

func (vl BytesValue) pack(packer *packer) error {
	packer.PackBytes(vl)
	return nil
}

// GetType returns wire protocol value type.
func (vl BytesValue) GetType() int {
	return ParticleType.BLOB
}

// GetObject returns original value as an interface{}.
func (vl BytesValue) GetObject() interface{} {
	return []byte(vl)
}

func (vl BytesValue) reader() io.Reader {
	return bytes.NewReader(vl)
}

// String implements Stringer interface.
func (vl BytesValue) String() string {
	return Buffer.BytesToHexString(vl)
}

///////////////////////////////////////////////////////////////////////////////

// StringValue encapsulates a string value.
type StringValue string

// NewStringValue generates a StringValue instance.
func NewStringValue(value string) StringValue {
	return StringValue(value)
}

func (vl StringValue) estimateSize() int {
	return len(vl)
}

func (vl StringValue) write(buffer []byte, offset int) (int, error) {
	return copy(buffer[offset:], vl), nil
}

func (vl StringValue) pack(packer *packer) error {
	packer.PackString(string(vl))
	return nil
}

// GetType returns wire protocol value type.
func (vl StringValue) GetType() int {
	return ParticleType.STRING
}

// GetObject returns original value as an interface{}.
func (vl StringValue) GetObject() interface{} {
	return string(vl)
}

func (vl StringValue) reader() io.Reader {
	return strings.NewReader(string(vl))
}

// String implements Stringer interface.
func (vl StringValue) String() string {
	return string(vl)
}

///////////////////////////////////////////////////////////////////////////////

// IntegerValue encapsulates an integer value.
type IntegerValue int

// NewIntegerValue generates an IntegerValue instance.
func NewIntegerValue(value int) IntegerValue {
	return IntegerValue(value)
}

func (vl IntegerValue) estimateSize() int {
	return 8
}

func (vl IntegerValue) write(buffer []byte, offset int) (int, error) {
	Buffer.Int64ToBytes(int64(vl), buffer, offset)
	return 8, nil
}

func (vl IntegerValue) pack(packer *packer) error {
	if Buffer.SizeOfInt == Buffer.SizeOfInt32 {
		packer.PackAInt(int(vl))
	} else {
		packer.PackALong(int64(vl))
	}
	return nil
}

// GetType returns wire protocol value type.
func (vl IntegerValue) GetType() int {
	return ParticleType.INTEGER
}

// GetObject returns original value as an interface{}.
func (vl IntegerValue) GetObject() interface{} {
	return int(vl)
}

func (vl IntegerValue) reader() io.Reader {
	return bytes.NewReader(Buffer.Int64ToBytes(int64(vl), nil, 0))
}

// String implements Stringer interface.
func (vl IntegerValue) String() string {
	return strconv.Itoa(int(vl))
}

///////////////////////////////////////////////////////////////////////////////

// LongValue encapsulates an int64 value.
type LongValue int64

// NewLongValue generates a LongValue instance.
func NewLongValue(value int64) LongValue {
	return LongValue(value)
}

func (vl LongValue) estimateSize() int {
	return 8
}

func (vl LongValue) write(buffer []byte, offset int) (int, error) {
	Buffer.Int64ToBytes(int64(vl), buffer, offset)
	return 8, nil
}

func (vl LongValue) pack(packer *packer) error {
	packer.PackALong(int64(vl))
	return nil
}

// GetType returns wire protocol value type.
func (vl LongValue) GetType() int {
	return ParticleType.INTEGER
}

// GetObject returns original value as an interface{}.
func (vl LongValue) GetObject() interface{} {
	return int64(vl)
}

func (vl LongValue) reader() io.Reader {
	return bytes.NewReader(Buffer.Int64ToBytes(int64(vl), nil, 0))
}

// String implements Stringer interface.
func (vl LongValue) String() string {
	return strconv.Itoa(int(vl))
}

///////////////////////////////////////////////////////////////////////////////

// FloatValue encapsulates an float64 value.
type FloatValue float64

// NewFloatValue generates a FloatValue instance.
func NewFloatValue(value float64) FloatValue {
	return FloatValue(value)
}

func (vl FloatValue) estimateSize() int {
	return 8
}

func (vl FloatValue) write(buffer []byte, offset int) (int, error) {
	Buffer.Float64ToBytes(float64(vl), buffer, offset)
	return 8, nil
}

func (vl FloatValue) pack(packer *packer) error {
	packer.PackFloat64(float64(vl))
	return nil
}

// GetType returns wire protocol value type.
func (vl FloatValue) GetType() int {
	return ParticleType.FLOAT
}

// GetObject returns original value as an interface{}.
func (vl FloatValue) GetObject() interface{} {
	return float64(vl)
}

func (vl FloatValue) reader() io.Reader {
	return bytes.NewReader(Buffer.Float64ToBytes(float64(vl), nil, 0))
}

// String implements Stringer interface.
func (vl FloatValue) String() string {
	return (fmt.Sprintf("%f", vl))
}

///////////////////////////////////////////////////////////////////////////////

// ValueArray encapsulates an array of Value.
// Supported by Aerospike 3 servers only.
type ValueArray struct {
	array []Value
	bytes []byte
}

// ToValueSlice converts a []interface{} to []Value.
// It will panic if any of array element types are not supported.
func ToValueSlice(array []interface{}) []Value {
	res := make([]Value, 0, len(array))
	for i := range array {
		res = append(res, NewValue(array[i]))
	}
	return res
}

// ToValueArray converts a []interface{} to a ValueArray type.
// It will panic if any of array element types are not supported.
func ToValueArray(array []interface{}) *ValueArray {
	return NewValueArray(ToValueSlice(array))
}

// NewValueArray generates a ValueArray instance.
func NewValueArray(array []Value) *ValueArray {
	res := &ValueArray{
		array: array,
	}

	res.bytes, _ = packValueArray(array)

	return res
}

func (vl *ValueArray) estimateSize() int {
	return len(vl.bytes)
}

func (vl *ValueArray) write(buffer []byte, offset int) (int, error) {
	res := copy(buffer[offset:], vl.bytes)
	return res, nil
}

func (vl *ValueArray) pack(packer *packer) error {
	return packer.packValueArray(vl.array)
}

// GetType returns wire protocol value type.
func (vl *ValueArray) GetType() int {
	return ParticleType.LIST
}

// GetObject returns original value as an interface{}.
func (vl *ValueArray) GetObject() interface{} {
	return vl.array
}

func (vl *ValueArray) reader() io.Reader {
	return bytes.NewReader(vl.bytes)
}

// String implements Stringer interface.
func (vl *ValueArray) String() string {
	return fmt.Sprintf("%v", vl.array)
}

///////////////////////////////////////////////////////////////////////////////

// ListValue encapsulates any arbitray array.
// Supported by Aerospike 3 servers only.
type ListValue struct {
	list  []interface{}
	bytes []byte
}

// NewListValue generates a ListValue instance.
func NewListValue(list []interface{}) *ListValue {
	res := &ListValue{
		list: list,
	}

	res.bytes, _ = packAnyArray(list)

	return res
}

func (vl *ListValue) estimateSize() int {
	return len(vl.bytes)
}

func (vl *ListValue) write(buffer []byte, offset int) (int, error) {
	l := copy(buffer[offset:], vl.bytes)
	return l, nil
}

func (vl *ListValue) pack(packer *packer) error {
	// return packer.PackList(vl.list)
	_, err := packer.buffer.Write(vl.bytes)
	return err
}

// GetType returns wire protocol value type.
func (vl *ListValue) GetType() int {
	return ParticleType.LIST
}

// GetObject returns original value as an interface{}.
func (vl *ListValue) GetObject() interface{} {
	return vl.list
}

func (vl *ListValue) reader() io.Reader {
	return bytes.NewReader(vl.bytes)
}

// String implements Stringer interface.
func (vl *ListValue) String() string {
	return fmt.Sprintf("%v", vl.list)
}

///////////////////////////////////////////////////////////////////////////////

// MapValue encapsulates an arbitray map.
// Supported by Aerospike 3 servers only.
type MapValue struct {
	vmap  map[interface{}]interface{}
	bytes []byte
}

// NewMapValue generates a MapValue instance.
func NewMapValue(vmap map[interface{}]interface{}) *MapValue {
	res := &MapValue{
		vmap: vmap,
	}

	res.bytes, _ = packAnyMap(vmap)

	return res
}

func (vl *MapValue) estimateSize() int {
	return len(vl.bytes)
}

func (vl *MapValue) write(buffer []byte, offset int) (int, error) {
	return copy(buffer[offset:], vl.bytes), nil
}

func (vl *MapValue) pack(packer *packer) error {
	// return packer.PackMap(vl.vmap)
	_, err := packer.buffer.Write(vl.bytes)
	return err
}

// GetType returns wire protocol value type.
func (vl *MapValue) GetType() int {
	return ParticleType.MAP
}

// GetObject returns original value as an interface{}.
func (vl *MapValue) GetObject() interface{} {
	return vl.vmap
}

func (vl *MapValue) reader() io.Reader {
	return bytes.NewReader(vl.bytes)
}

// String implements Stringer interface.
func (vl *MapValue) String() string {
	return fmt.Sprintf("%v", vl.vmap)
}

///////////////////////////////////////////////////////////////////////////////

// MapValue encapsulates a 2D Geo point.
// Supported by Aerospike 3.6.1 servers only.
type GeoJSONValue string

// NewMapValue generates a GeoJSONValue instance.
func NewGeoJSONValue(value string) GeoJSONValue {
	res := GeoJSONValue(value)
	return res
}

func (vl GeoJSONValue) estimateSize() int {
	// flags + ncells + jsonstr
	return 1 + 2 + len(string(vl))
}

func (vl GeoJSONValue) write(buffer []byte, offset int) (int, error) {
	buffer[offset] = 0   // flags
	buffer[offset+1] = 0 // flags
	buffer[offset+2] = 0 // flags

	return 1 + 2 + copy(buffer[offset+3:], string(vl)), nil
}

func (vl GeoJSONValue) pack(packer *packer) error {
	packer.PackGeoJson(string(vl))
	return nil
}

// GetType returns wire protocol value type.
func (vl GeoJSONValue) GetType() int {
	return ParticleType.GEOJSON
}

// GetObject returns original value as an interface{}.
func (vl GeoJSONValue) GetObject() interface{} {
	return string(vl)
}

func (vl GeoJSONValue) reader() io.Reader {
	return strings.NewReader(string(vl))
}

// String implements Stringer interface.
func (vl GeoJSONValue) String() string {
	return string(vl)
}

//////////////////////////////////////////////////////////////////////////////

func bytesToParticle(ptype int, buf []byte, offset int, length int) (interface{}, error) {

	switch ptype {
	case ParticleType.INTEGER:
		return Buffer.BytesToNumber(buf, offset, length), nil

	case ParticleType.FLOAT:
		return Buffer.BytesToFloat64(buf, offset), nil

	case ParticleType.STRING:
		return string(buf[offset : offset+length]), nil

	case ParticleType.BLOB:
		newObj := make([]byte, length)
		copy(newObj, buf[offset:offset+length])
		return newObj, nil

	case ParticleType.LIST:
		return newUnpacker(buf, offset, length).UnpackList()

	case ParticleType.MAP:
		return newUnpacker(buf, offset, length).UnpackMap()

	case ParticleType.LDT:
		return newUnpacker(buf, offset, length).unpackObjects()

	case ParticleType.GEOJSON:
		ncells := int(Buffer.BytesToInt16(buf, offset+1))
		headerSize := 1 + 2 + (ncells * 8)
		return string(buf[offset+headerSize : offset+length]), nil
	}
	return nil, nil
}

func bytesToKeyValue(pType int, buf []byte, offset int, len int) (Value, error) {

	switch pType {
	case ParticleType.STRING:
		return NewStringValue(string(buf[offset : offset+len])), nil

	case ParticleType.INTEGER:
		return NewLongValue(Buffer.VarBytesToInt64(buf, offset, len)), nil

	case ParticleType.FLOAT:
		return NewFloatValue(Buffer.BytesToFloat64(buf, offset)), nil

	case ParticleType.BLOB:
		bytes := make([]byte, len, len)
		copy(bytes, buf[offset:offset+len])
		return NewBytesValue(bytes), nil

	default:
		return nil, nil
	}
}

func unwrapValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	if uv, ok := v.(Value); ok {
		return unwrapValue(uv.GetObject())
	} else if uv, ok := v.([]Value); ok {
		a := make([]interface{}, len(uv))
		for i := range uv {
			a[i] = unwrapValue(uv[i].GetObject())
		}
		return a
	}

	return v
}
