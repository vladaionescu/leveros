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

// OperationType determines operation type
type OperationType byte

// Valid OperationType values that can be used to create custom Operations.
// The names are self-explanatory.
const (
	READ OperationType = 1
	// READ_HEADER OperationType = 1

	WRITE      OperationType = 2
	ADD        OperationType = 5
	CDT_READ   OperationType = 3
	CDT_MODIFY OperationType = 4
	APPEND     OperationType = 9
	PREPEND    OperationType = 10
	TOUCH      OperationType = 11
)

// Operation contasins operation definition.
// This struct is used in client's operate() method.
type Operation struct {

	// OpType determines type of operation.
	OpType OperationType

	// BinName (Optional) determines the name of bin used in operation.
	BinName string

	// BinValue (Optional) determines bin value used in operation.
	BinValue Value

	// will be true ONLY for GetHeader() operation
	headerOnly bool
}

// GetOpForBin creates read bin database operation.
func GetOpForBin(binName string) *Operation {
	return &Operation{OpType: READ, BinName: binName, BinValue: NewNullValue()}
}

// GetOp creates read all record bins database operation.
func GetOp() *Operation {
	return &Operation{OpType: READ, BinValue: NewNullValue()}
}

// GetHeaderOp creates read record header database operation.
func GetHeaderOp() *Operation {
	return &Operation{OpType: READ, headerOnly: true, BinValue: NewNullValue()}
}

// PutOp creates set database operation.
func PutOp(bin *Bin) *Operation {
	return &Operation{OpType: WRITE, BinName: bin.Name, BinValue: bin.Value}
}

// AppendOp creates string append database operation.
func AppendOp(bin *Bin) *Operation {
	return &Operation{OpType: APPEND, BinName: bin.Name, BinValue: bin.Value}
}

// PrependOp creates string prepend database operation.
func PrependOp(bin *Bin) *Operation {
	return &Operation{OpType: PREPEND, BinName: bin.Name, BinValue: bin.Value}
}

// AddOp creates integer add database operation.
func AddOp(bin *Bin) *Operation {
	return &Operation{OpType: ADD, BinName: bin.Name, BinValue: bin.Value}
}

// TouchOp creates touch database operation.
func TouchOp() *Operation {
	return &Operation{OpType: TOUCH, BinValue: NewNullValue()}
}

/*
	CDT List operations Go Here
*/

// List bin operations. Create list operations used by client.Operate command.
// List operations support negative indexing.  If the index is negative, the
// resolved index starts backwards from end of list.
//
// Index/Range examples:
//
//    Index 0: First item in list.
//    Index 4: Fifth item in list.
//    Index -1: Last item in list.
//    Index -3: Third to last item in list.
//    Index 1 Count 2: Second and third items in list.
//    Index -3 Count 3: Last three items in list.
//    Index -5 Count 4: Range between fifth to last item to second to last item inclusive.
//
// If an index is out of bounds, a parameter error will be returned. If a range is partially
// out of bounds, the valid part of the range will be returned.

const (
	_CDT_LIST_APPEND       = 1
	_CDT_LIST_APPEND_ITEMS = 2
	_CDT_LIST_INSERT       = 3
	_CDT_LIST_INSERT_ITEMS = 4
	_CDT_LIST_POP          = 5
	_CDT_LIST_POP_RANGE    = 6
	_CDT_LIST_REMOVE       = 7
	_CDT_LIST_REMOVE_RANGE = 8
	_CDT_LIST_SET          = 9
	_CDT_LIST_TRIM         = 10
	_CDT_LIST_CLEAR        = 11
	_CDT_LIST_SIZE         = 16
	_CDT_LIST_GET          = 17
	_CDT_LIST_GET_RANGE    = 18
)

// ListAppendOp creates a list append operation.
// Server appends values to end of list bin.
// Server returns list size on bin name.
// It will panic is no values have been passed.
func ListAppendOp(binName string, values ...interface{}) *Operation {
	list := ToValueSlice(values)
	if len(values) == 1 {
		packer := newPacker()
		packer.PackShortRaw(_CDT_LIST_APPEND)
		packer.PackArrayBegin(1)
		list[0].pack(packer)
		bytes := packer.buffer.Bytes()
		return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
	} else if len(values) > 1 {
		packer := newPacker()
		packer.PackShortRaw(_CDT_LIST_APPEND_ITEMS)
		packer.PackArrayBegin(1)
		packer.packValueArray(list)
		bytes := packer.buffer.Bytes()
		return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
	}

	panic("At least one value must be provided")
}

// ListInsertOp creates a list insert operation.
// Server inserts value to specified index of list bin.
// Server returns list size on bin name.
// It will panic is no values have been passed.
func ListInsertOp(binName string, index int, values ...interface{}) *Operation {
	list := ToValueSlice(values)
	if len(values) == 1 {
		packer := newPacker()
		packer.PackShortRaw(_CDT_LIST_INSERT)
		packer.PackArrayBegin(2)
		packer.PackAInt(index)
		list[0].pack(packer)
		bytes := packer.buffer.Bytes()
		return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
	} else if len(values) > 1 {
		packer := newPacker()
		packer.PackShortRaw(_CDT_LIST_INSERT_ITEMS)
		packer.PackArrayBegin(2)
		packer.PackAInt(index)
		packer.packValueArray(list)
		bytes := packer.buffer.Bytes()
		return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
	}

	panic("At least one value must be provided")
}

// ListPopOp creates list pop operation.
// Server returns item at specified index and removes item from list bin.
func ListPopOp(binName string, index int) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_POP)
	packer.PackArrayBegin(1)
	packer.PackAInt(index)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
}

// ListPopRangeOp creates a list pop range operation.
// Server returns items starting at specified index and removes items from list bin.
func ListPopRangeOp(binName string, index int, count int) *Operation {
	if count == 1 {
		return ListPopOp(binName, index)
	}

	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_POP_RANGE)
	packer.PackArrayBegin(2)
	packer.PackAInt(index)
	packer.PackAInt(count)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
}

// ListPopRangeFromOp creates a list pop range operation.
// Server returns items starting at specified index to the end of list and removes items from list bin.
func ListPopRangeFromOp(binName string, index int) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_POP_RANGE)
	packer.PackArrayBegin(1)
	packer.PackAInt(index)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
}

// ListRemoveOp creates a list remove operation.
// Server removes item at specified index from list bin.
// Server returns number of items removed.
func ListRemoveOp(binName string, index int) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_REMOVE)
	packer.PackArrayBegin(1)
	packer.PackAInt(index)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
}

// ListRemoveRangeOp creates a list remove range operation.
// Server removes "count" items starting at specified index from list bin.
// Server returns number of items removed.
func ListRemoveRangeOp(binName string, index int, count int) *Operation {
	if count == 1 {
		return ListRemoveOp(binName, index)
	}

	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_REMOVE_RANGE)
	packer.PackArrayBegin(2)
	packer.PackAInt(index)
	packer.PackAInt(count)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
}

// ListRemoveRangeFromOp creates a list remove range operation.
// Server removes all items starting at specified index to the end of list.
// Server returns number of items removed.
func ListRemoveRangeFromOp(binName string, index int) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_REMOVE_RANGE)
	packer.PackArrayBegin(1)
	packer.PackAInt(index)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
}

// ListSetOp creates a list set operation.
// Server sets item value at specified index in list bin.
// Server does not return a result by default.
func ListSetOp(binName string, index int, value interface{}) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_SET)
	packer.PackArrayBegin(2)
	packer.PackAInt(index)
	NewValue(value).pack(packer)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
}

// ListTrimOp creates a list trim operation.
// Server removes "count" items in list bin that do not fall into range specified
// by index and count range.  If the range is out of bounds, then all items will be removed.
// Server returns number of elemts that were removed.
func ListTrimOp(binName string, index int, count int) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_TRIM)
	packer.PackArrayBegin(2)
	packer.PackAInt(index)
	packer.PackAInt(count)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
}

// ListClearOp creates a list clear operation.
// Server removes all items in list bin.
// Server does not return a result by default.
func ListClearOp(binName string) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_CLEAR)
	//packer.PackArrayBegin(0);
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_MODIFY, BinName: binName, BinValue: NewValue(bytes)}
}

// ListSizeOp creates a list size operation.
// Server returns size of list on bin name.
func ListSizeOp(binName string) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_SIZE)
	//packer.PackArrayBegin(0);
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_READ, BinName: binName, BinValue: NewValue(bytes)}
}

// ListGetOp creates a list get operation.
// Server returns item at specified index in list bin.
func ListGetOp(binName string, index int) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_GET)
	packer.PackArrayBegin(1)
	packer.PackAInt(index)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_READ, BinName: binName, BinValue: NewValue(bytes)}
}

// ListGetRangeOp creates a list get range operation.
// Server returns "count" items starting at specified index in list bin.
func ListGetRangeOp(binName string, index int, count int) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_GET_RANGE)
	packer.PackArrayBegin(2)
	packer.PackAInt(index)
	packer.PackAInt(count)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_READ, BinName: binName, BinValue: NewValue(bytes)}
}

// ListGetRangeFromOp creates a list get range operation.
// Server returns items starting at specified index to the end of list.
func ListGetRangeFromOp(binName string, index int) *Operation {
	packer := newPacker()
	packer.PackShortRaw(_CDT_LIST_GET_RANGE)
	packer.PackArrayBegin(1)
	packer.PackAInt(index)
	bytes := packer.buffer.Bytes()
	return &Operation{OpType: CDT_READ, BinName: binName, BinValue: NewValue(bytes)}
}
