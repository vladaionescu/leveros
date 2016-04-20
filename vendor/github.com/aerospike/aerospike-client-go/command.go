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
	"errors"
	"fmt"
	"time"

	. "github.com/aerospike/aerospike-client-go/logger"

	. "github.com/aerospike/aerospike-client-go/types"
	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

const (
	// Flags commented out are not supported by cmd client.
	// Contains a read operation.
	_INFO1_READ int = (1 << 0)
	// Get all bins.
	_INFO1_GET_ALL int = (1 << 1)

	// Do not read the bins
	_INFO1_NOBINDATA int = (1 << 5)

	// Involve all replicas in read operation.
	_INFO1_CONSISTENCY_ALL = (1 << 6)

	// Create or update record
	_INFO2_WRITE int = (1 << 0)
	// Fling a record into the belly of Moloch.
	_INFO2_DELETE int = (1 << 1)
	// Update if expected generation == old.
	_INFO2_GENERATION int = (1 << 2)
	// Update if new generation >= old, good for restore.
	_INFO2_GENERATION_GT int = (1 << 3)
	// Create a duplicate on a generation collision.
	_INFO2_GENERATION_DUP int = (1 << 4)
	// Create only. Fail if record already exists.
	_INFO2_CREATE_ONLY int = (1 << 5)

	// Return a result for every operation.
	_INFO2_RESPOND_ALL_OPS int = (1 << 7)

	// This is the last of a multi-part message.
	_INFO3_LAST int = (1 << 0)
	// Commit to master only before declaring success.
	_INFO3_COMMIT_MASTER int = (1 << 1)
	// Update only. Merge bins.
	_INFO3_UPDATE_ONLY int = (1 << 3)

	// Create or completely replace record.
	_INFO3_CREATE_OR_REPLACE int = (1 << 4)
	// Completely replace existing record only.
	_INFO3_REPLACE_ONLY int = (1 << 5)

	_MSG_TOTAL_HEADER_SIZE     uint8 = 30
	_FIELD_HEADER_SIZE         uint8 = 5
	_OPERATION_HEADER_SIZE     uint8 = 8
	_MSG_REMAINING_HEADER_SIZE uint8 = 22
	_DIGEST_SIZE               uint8 = 20
	_CL_MSG_VERSION            int64 = 2
	_AS_MSG_TYPE               int64 = 3
)

// command intrerface describes all commands available
type command interface {
	getPolicy(ifc command) Policy

	setConnection(conn *Connection)
	getConnection() *Connection

	writeBuffer(ifc command) error
	getNode(ifc command) (*Node, error)
	parseResult(ifc command, conn *Connection) error
	parseRecordResults(ifc command, receiveSize int) (bool, error)

	execute(ifc command) error
	// Executes the command
	Execute() error
}

// Holds data buffer for the command
type baseCommand struct {
	node *Node
	conn *Connection

	dataBuffer []byte
	dataOffset int
}

// Writes the command for write operations
func (cmd *baseCommand) setWrite(policy *WritePolicy, operation OperationType, key *Key, bins []*Bin) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key, policy.SendKey)

	for i := range bins {
		cmd.estimateOperationSizeForBin(bins[i])
	}
	if err := cmd.sizeBuffer(); err != nil {
		return err
	}
	cmd.writeHeaderWithPolicy(policy, 0, _INFO2_WRITE, fieldCount, len(bins))
	cmd.writeKey(key, policy.SendKey)

	for i := range bins {
		if err := cmd.writeOperationForBin(bins[i], operation); err != nil {
			return err
		}
	}
	cmd.end()

	return nil
}

// Writes the command for delete operations
func (cmd *baseCommand) setDelete(policy *WritePolicy, key *Key) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key, false)
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeaderWithPolicy(policy, 0, _INFO2_WRITE|_INFO2_DELETE, fieldCount, 0)
	cmd.writeKey(key, false)
	cmd.end()
	return nil

}

// Writes the command for touch operations
func (cmd *baseCommand) setTouch(policy *WritePolicy, key *Key) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key, policy.SendKey)

	cmd.estimateOperationSize()
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeaderWithPolicy(policy, 0, _INFO2_WRITE, fieldCount, 1)
	cmd.writeKey(key, policy.SendKey)
	cmd.writeOperationForOperationType(TOUCH)
	cmd.end()
	return nil

}

// Writes the command for exist operations
func (cmd *baseCommand) setExists(policy *BasePolicy, key *Key) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key, false)
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeader(policy.GetBasePolicy(), _INFO1_READ|_INFO1_NOBINDATA, 0, fieldCount, 0)
	cmd.writeKey(key, false)
	cmd.end()
	return nil

}

// Writes the command for get operations (all bins)
func (cmd *baseCommand) setReadForKeyOnly(policy *BasePolicy, key *Key) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key, false)
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeader(policy, _INFO1_READ|_INFO1_GET_ALL, 0, fieldCount, 0)
	cmd.writeKey(key, false)
	cmd.end()
	return nil

}

// Writes the command for get operations (specified bins)
func (cmd *baseCommand) setRead(policy *BasePolicy, key *Key, binNames []string) (err error) {
	if binNames != nil && len(binNames) > 0 {
		cmd.begin()
		fieldCount := cmd.estimateKeySize(key, false)

		for i := range binNames {
			cmd.estimateOperationSizeForBinName(binNames[i])
		}
		if err = cmd.sizeBuffer(); err != nil {
			return nil
		}
		cmd.writeHeader(policy.GetBasePolicy(), _INFO1_READ, 0, fieldCount, len(binNames))
		cmd.writeKey(key, false)

		for i := range binNames {
			cmd.writeOperationForBinName(binNames[i], READ)
		}
		cmd.end()
	} else {
		err = cmd.setReadForKeyOnly(policy, key)
	}

	return err
}

// Writes the command for getting metadata operations
func (cmd *baseCommand) setReadHeader(policy *BasePolicy, key *Key) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key, false)
	cmd.estimateOperationSizeForBinName("")
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}

	cmd.writeHeader(policy.GetBasePolicy(), _INFO1_READ|_INFO1_NOBINDATA, 0, fieldCount, 1)

	cmd.writeKey(key, false)
	cmd.writeOperationForBinName("", READ)
	cmd.end()
	return nil

}

// Implements different command operations
func (cmd *baseCommand) setOperate(policy *WritePolicy, key *Key, operations []*Operation) error {
	cmd.begin()
	fieldCount := 0
	readAttr := 0
	writeAttr := 0
	readBin := false
	readHeader := false

	for i := range operations {
		switch operations[i].OpType {
		case READ, CDT_READ:
			if !operations[i].headerOnly {
				readAttr |= _INFO1_READ

				// Read all bins if no bin is specified.
				if operations[i].BinName == "" {
					readAttr |= _INFO1_GET_ALL
				}
				readBin = true
			} else {
				readAttr |= _INFO1_READ
				readHeader = true
			}
		default:
			writeAttr = _INFO2_WRITE
		}
		cmd.estimateOperationSizeForOperation(operations[i])
	}

	fieldCount = cmd.estimateKeySize(key, policy.SendKey && writeAttr != 0)

	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}

	if readHeader && !readBin {
		readAttr |= _INFO1_NOBINDATA
	}

	if writeAttr != 0 && policy.RespondPerEachOp {
		writeAttr |= _INFO2_RESPOND_ALL_OPS
	}

	if writeAttr != 0 {
		cmd.writeHeaderWithPolicy(policy, readAttr, writeAttr, fieldCount, len(operations))
	} else {
		cmd.writeHeader(policy.GetBasePolicy(), readAttr, writeAttr, fieldCount, len(operations))
	}
	cmd.writeKey(key, policy.SendKey && writeAttr != 0)

	for _, operation := range operations {
		if err := cmd.writeOperationForOperation(operation); err != nil {
			return err
		}
	}

	cmd.end()

	return nil
}

func (cmd *baseCommand) setUdf(policy *WritePolicy, key *Key, packageName string, functionName string, args []Value) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key, policy.SendKey)
	argBytes, err := packValueArray(args)
	if err != nil {
		return err
	}

	fieldCount += cmd.estimateUdfSize(packageName, functionName, argBytes)
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeader(policy.GetBasePolicy(), 0, _INFO2_WRITE, fieldCount, 0)
	cmd.writeKey(key, policy.SendKey)
	cmd.writeFieldString(packageName, UDF_PACKAGE_NAME)
	cmd.writeFieldString(functionName, UDF_FUNCTION)
	cmd.writeFieldBytes(argBytes, UDF_ARGLIST)
	cmd.end()

	return nil
}

func (cmd *baseCommand) setBatchExists(policy *BasePolicy, keys []*Key, batch *batchNamespace) error {
	// Estimate buffer size
	cmd.begin()
	byteSize := batch.offsetSize * int(_DIGEST_SIZE)

	cmd.dataOffset += len(*batch.namespace) +
		int(_FIELD_HEADER_SIZE) + byteSize + int(_FIELD_HEADER_SIZE)
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}

	cmd.writeHeader(policy, _INFO1_READ|_INFO1_NOBINDATA, 0, 2, 0)
	cmd.writeFieldString(*batch.namespace, NAMESPACE)
	cmd.writeFieldHeader(byteSize, DIGEST_RIPE_ARRAY)

	offsets := batch.offsets
	max := batch.offsetSize

	for i := 0; i < max; i++ {
		key := keys[offsets[i]]
		copy(cmd.dataBuffer[cmd.dataOffset:], key.digest)
		cmd.dataOffset += len(key.digest)
	}
	cmd.end()

	return nil
}

func (cmd *baseCommand) setBatchGet(policy Policy, keys []*Key, batch *batchNamespace, binNames map[string]struct{}, readAttr int) error {
	// Estimate buffer size
	cmd.begin()
	byteSize := batch.offsetSize * int(_DIGEST_SIZE)

	cmd.dataOffset += len(*batch.namespace) +
		int(_FIELD_HEADER_SIZE) + byteSize + int(_FIELD_HEADER_SIZE)

	for binName := range binNames {
		cmd.estimateOperationSizeForBinName(binName)
	}

	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}

	operationCount := len(binNames)
	cmd.writeHeader(policy.GetBasePolicy(), readAttr, 0, 2, operationCount)
	cmd.writeFieldString(*batch.namespace, NAMESPACE)
	cmd.writeFieldHeader(byteSize, DIGEST_RIPE_ARRAY)

	offsets := batch.offsets
	max := batch.offsetSize

	for i := 0; i < max; i++ {
		key := keys[offsets[i]]
		copy(cmd.dataBuffer[cmd.dataOffset:], key.digest)
		cmd.dataOffset += len(key.digest)
	}

	for binName := range binNames {
		cmd.writeOperationForBinName(binName, READ)
	}
	cmd.end()

	return nil
}

func (cmd *baseCommand) setScan(policy *ScanPolicy, namespace *string, setName *string, binNames []string) error {
	cmd.begin()
	fieldCount := 0

	if namespace != nil {
		cmd.dataOffset += len(*namespace) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	if setName != nil {
		cmd.dataOffset += len(*setName) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	// Estimate scan options size.
	cmd.dataOffset += 2 + int(_FIELD_HEADER_SIZE)
	fieldCount++

	if binNames != nil {
		for i := range binNames {
			cmd.estimateOperationSizeForBinName(binNames[i])
		}
	}
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	readAttr := _INFO1_READ

	if !policy.IncludeBinData {
		readAttr |= _INFO1_NOBINDATA
	}

	operationCount := 0
	if binNames != nil {
		operationCount = len(binNames)
	}
	cmd.writeHeader(policy.GetBasePolicy(), readAttr, 0, fieldCount, operationCount)

	if namespace != nil {
		cmd.writeFieldString(*namespace, NAMESPACE)
	}

	if setName != nil {
		cmd.writeFieldString(*setName, TABLE)
	}

	cmd.writeFieldHeader(2, SCAN_OPTIONS)
	priority := byte(policy.Priority)
	priority <<= 4

	if policy.FailOnClusterChange {
		priority |= 0x08
	}

	if policy.IncludeLDT {
		priority |= 0x02
	}

	cmd.dataBuffer[cmd.dataOffset] = priority
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = byte(policy.ScanPercent)
	cmd.dataOffset++

	if binNames != nil {
		for i := range binNames {
			cmd.writeOperationForBinName(binNames[i], READ)
		}
	}
	cmd.end()

	return nil
}

func (cmd *baseCommand) setQuery(policy *QueryPolicy, statement *Statement, write bool) (err error) {
	var functionArgBuffer []byte

	fieldCount := 0
	filterSize := 0
	binNameSize := 0

	cmd.begin()

	if statement.Namespace != "" {
		cmd.dataOffset += len(statement.Namespace) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	if statement.IndexName != "" {
		cmd.dataOffset += len(statement.IndexName) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	if statement.SetName != "" {
		cmd.dataOffset += len(statement.SetName) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	// Allocate space for TaskId field.
	cmd.dataOffset += 8 + int(_FIELD_HEADER_SIZE)
	fieldCount++

	if len(statement.Filters) > 0 {
		if len(statement.Filters) > 1 {
			return NewAerospikeError(PARAMETER_ERROR, "Aerospike server currently supports only one filter.")
		} else if len(statement.Filters) == 1 {
			idxType := statement.Filters[0].IndexCollectionType()

			if idxType != ICT_DEFAULT {
				cmd.dataOffset += int(_FIELD_HEADER_SIZE) + 1
				fieldCount++
			}
		}

		cmd.dataOffset += int(_FIELD_HEADER_SIZE)
		filterSize++ // num filters

		for _, filter := range statement.Filters {
			sz, err := filter.estimateSize()
			if err != nil {
				return err
			}
			filterSize += sz
		}
		cmd.dataOffset += filterSize
		fieldCount++

		// Query bin names are specified as a field (Scan bin names are specified later as operations)
		if len(statement.BinNames) > 0 {
			cmd.dataOffset += int(_FIELD_HEADER_SIZE)
			binNameSize++ // num bin names

			for _, binName := range statement.BinNames {
				binNameSize += len(binName) + 1
			}
			cmd.dataOffset += binNameSize
			fieldCount++
		}
	} else {
		// Calling query with no filters is more efficiently handled by a primary index scan.
		// Estimate scan options size.
		cmd.dataOffset += (2 + int(_FIELD_HEADER_SIZE))
		fieldCount++
	}

	if statement.functionName != "" {
		cmd.dataOffset += int(_FIELD_HEADER_SIZE) + 1 // udf type
		cmd.dataOffset += len(statement.packageName) + int(_FIELD_HEADER_SIZE)
		cmd.dataOffset += len(statement.functionName) + int(_FIELD_HEADER_SIZE)

		if len(statement.functionArgs) > 0 {
			functionArgBuffer, err = packValueArray(statement.functionArgs)
			if err != nil {
				return err
			}
		} else {
			functionArgBuffer = []byte{}
		}
		cmd.dataOffset += int(_FIELD_HEADER_SIZE) + len(functionArgBuffer)
		fieldCount += 4
	}

	if len(statement.Filters) == 0 {
		if len(statement.BinNames) > 0 {
			for _, binName := range statement.BinNames {
				cmd.estimateOperationSizeForBinName(binName)
			}
		}
	}

	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}

	operationCount := 0
	if len(statement.Filters) == 0 && len(statement.BinNames) > 0 {
		operationCount = len(statement.BinNames)
	}

	if write {
		cmd.writeHeader(policy.BasePolicy, _INFO1_READ, _INFO2_WRITE, fieldCount, operationCount)
	} else {
		cmd.writeHeader(policy.BasePolicy, _INFO1_READ, 0, fieldCount, operationCount)
	}

	if statement.Namespace != "" {
		cmd.writeFieldString(statement.Namespace, NAMESPACE)
	}

	if statement.IndexName != "" {
		cmd.writeFieldString(statement.IndexName, INDEX_NAME)
	}

	if statement.SetName != "" {
		cmd.writeFieldString(statement.SetName, TABLE)
	}

	cmd.writeFieldHeader(8, TRAN_ID)
	Buffer.Int64ToBytes(int64(statement.TaskId), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 8

	if len(statement.Filters) > 0 {
		if len(statement.Filters) >= 1 {
			idxType := statement.Filters[0].IndexCollectionType()

			if idxType != ICT_DEFAULT {
				cmd.writeFieldHeader(1, INDEX_TYPE)
				cmd.dataBuffer[cmd.dataOffset] = byte(idxType)
				cmd.dataOffset++
			}
		}

		cmd.writeFieldHeader(filterSize, INDEX_RANGE)
		cmd.dataBuffer[cmd.dataOffset] = byte(len(statement.Filters))
		cmd.dataOffset++

		for _, filter := range statement.Filters {
			cmd.dataOffset, err = filter.write(cmd.dataBuffer, cmd.dataOffset)
			if err != nil {
				return err
			}
		}

		if len(statement.BinNames) > 0 {
			cmd.writeFieldHeader(binNameSize, QUERY_BINLIST)
			cmd.dataBuffer[cmd.dataOffset] = byte(len(statement.BinNames))
			cmd.dataOffset++

			for _, binName := range statement.BinNames {
				len := copy(cmd.dataBuffer[cmd.dataOffset+1:], binName)
				cmd.dataBuffer[cmd.dataOffset] = byte(len)
				cmd.dataOffset += len + 1
			}
		}
	} else {
		// Calling query with no filters is more efficiently handled by a primary index scan.
		cmd.writeFieldHeader(2, SCAN_OPTIONS)
		priority := byte(policy.Priority)
		priority <<= 4
		cmd.dataBuffer[cmd.dataOffset] = priority
		cmd.dataOffset++
		cmd.dataBuffer[cmd.dataOffset] = byte(100)
		cmd.dataOffset++
	}

	if statement.functionName != "" {
		cmd.writeFieldHeader(1, UDF_OP)
		if statement.returnData {
			cmd.dataBuffer[cmd.dataOffset] = byte(1)
		} else {
			cmd.dataBuffer[cmd.dataOffset] = byte(2)
		}
		cmd.dataOffset++

		cmd.writeFieldString(statement.packageName, UDF_PACKAGE_NAME)
		cmd.writeFieldString(statement.functionName, UDF_FUNCTION)
		cmd.writeFieldBytes(functionArgBuffer, UDF_ARGLIST)
	}

	// scan binNames come last
	if len(statement.Filters) == 0 {
		if len(statement.BinNames) > 0 {
			for _, binName := range statement.BinNames {
				cmd.writeOperationForBinName(binName, READ)
			}
		}
	}

	cmd.end()

	return nil
}

func (cmd *baseCommand) estimateKeySize(key *Key, sendKey bool) int {
	fieldCount := 0

	if key.namespace != "" {
		cmd.dataOffset += len(key.namespace) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	if key.setName != "" {
		cmd.dataOffset += len(key.setName) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	cmd.dataOffset += int(_DIGEST_SIZE + _FIELD_HEADER_SIZE)
	fieldCount++

	if sendKey {
		// field header size + key size
		cmd.dataOffset += key.userKey.estimateSize() + int(_FIELD_HEADER_SIZE) + 1
		fieldCount++
	}

	return fieldCount
}

func (cmd *baseCommand) estimateUdfSize(packageName string, functionName string, bytes []byte) int {
	cmd.dataOffset += len(packageName) + int(_FIELD_HEADER_SIZE)
	cmd.dataOffset += len(functionName) + int(_FIELD_HEADER_SIZE)
	cmd.dataOffset += len(bytes) + int(_FIELD_HEADER_SIZE)
	return 3
}

func (cmd *baseCommand) estimateOperationSizeForBin(bin *Bin) {
	cmd.dataOffset += len(bin.Name) + int(_OPERATION_HEADER_SIZE)
	cmd.dataOffset += bin.Value.estimateSize()
}

func (cmd *baseCommand) estimateOperationSizeForOperation(operation *Operation) {
	binLen := len(operation.BinName)
	cmd.dataOffset += binLen + int(_OPERATION_HEADER_SIZE)

	if operation.BinValue != nil {
		cmd.dataOffset += operation.BinValue.estimateSize()
	}
}

func (cmd *baseCommand) estimateOperationSizeForBinName(binName string) {
	cmd.dataOffset += len(binName) + int(_OPERATION_HEADER_SIZE)
}

func (cmd *baseCommand) estimateOperationSize() {
	cmd.dataOffset += int(_OPERATION_HEADER_SIZE)
}

// Generic header write.
func (cmd *baseCommand) writeHeader(policy *BasePolicy, readAttr int, writeAttr int, fieldCount int, operationCount int) {

	if policy.ConsistencyLevel == CONSISTENCY_ALL {
		readAttr |= _INFO1_CONSISTENCY_ALL
	}

	// Write all header data except total size which must be written last.
	cmd.dataBuffer[8] = _MSG_REMAINING_HEADER_SIZE // Message header length.
	cmd.dataBuffer[9] = byte(readAttr)
	cmd.dataBuffer[10] = byte(writeAttr)

	for i := 11; i < 26; i++ {
		cmd.dataBuffer[i] = 0
	}
	Buffer.Int16ToBytes(int16(fieldCount), cmd.dataBuffer, 26)
	Buffer.Int16ToBytes(int16(operationCount), cmd.dataBuffer, 28)
	cmd.dataOffset = int(_MSG_TOTAL_HEADER_SIZE)
}

// Header write for write operations.
func (cmd *baseCommand) writeHeaderWithPolicy(policy *WritePolicy, readAttr int, writeAttr int, fieldCount int, operationCount int) {
	// Set flags.
	generation := uint32(0)
	infoAttr := 0

	switch policy.RecordExistsAction {
	case UPDATE:
	case UPDATE_ONLY:
		infoAttr |= _INFO3_UPDATE_ONLY
	case REPLACE:
		infoAttr |= _INFO3_CREATE_OR_REPLACE
	case REPLACE_ONLY:
		infoAttr |= _INFO3_REPLACE_ONLY
	case CREATE_ONLY:
		writeAttr |= _INFO2_CREATE_ONLY
	}

	switch policy.GenerationPolicy {
	case NONE:
	case EXPECT_GEN_EQUAL:
		generation = policy.Generation
		writeAttr |= _INFO2_GENERATION
	case EXPECT_GEN_GT:
		generation = policy.Generation
		writeAttr |= _INFO2_GENERATION_GT
	}

	if policy.CommitLevel == COMMIT_MASTER {
		infoAttr |= _INFO3_COMMIT_MASTER
	}

	if policy.ConsistencyLevel == CONSISTENCY_ALL {
		readAttr |= _INFO1_CONSISTENCY_ALL
	}

	// Write all header data except total size which must be written last.
	cmd.dataBuffer[8] = _MSG_REMAINING_HEADER_SIZE // Message header length.
	cmd.dataBuffer[9] = byte(readAttr)
	cmd.dataBuffer[10] = byte(writeAttr)
	cmd.dataBuffer[11] = byte(infoAttr)
	cmd.dataBuffer[12] = 0 // unused
	cmd.dataBuffer[13] = 0 // clear the result code
	Buffer.Uint32ToBytes(generation, cmd.dataBuffer, 14)
	Buffer.Uint32ToBytes(policy.Expiration, cmd.dataBuffer, 18)

	// Initialize timeout. It will be written later.
	cmd.dataBuffer[22] = 0
	cmd.dataBuffer[23] = 0
	cmd.dataBuffer[24] = 0
	cmd.dataBuffer[25] = 0

	Buffer.Int16ToBytes(int16(fieldCount), cmd.dataBuffer, 26)
	Buffer.Int16ToBytes(int16(operationCount), cmd.dataBuffer, 28)
	cmd.dataOffset = int(_MSG_TOTAL_HEADER_SIZE)
}

func (cmd *baseCommand) writeKey(key *Key, sendKey bool) {
	// Write key into buffer.
	if key.namespace != "" {
		cmd.writeFieldString(key.namespace, NAMESPACE)
	}

	if key.setName != "" {
		cmd.writeFieldString(key.setName, TABLE)
	}

	cmd.writeFieldBytes(key.digest[:], DIGEST_RIPE)

	if sendKey {
		cmd.writeFieldValue(key.userKey, KEY)
	}
}

func (cmd *baseCommand) writeOperationForBin(bin *Bin, operation OperationType) error {
	nameLength := copy(cmd.dataBuffer[(cmd.dataOffset+int(_OPERATION_HEADER_SIZE)):], bin.Name)

	// check for float support
	cmd.checkServerCompatibility(bin.Value)

	valueLength, err := bin.Value.write(cmd.dataBuffer, cmd.dataOffset+int(_OPERATION_HEADER_SIZE)+nameLength)
	if err != nil {
		return err
	}

	Buffer.Int32ToBytes(int32(nameLength+valueLength+4), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 4
	cmd.dataBuffer[cmd.dataOffset] = (byte(operation))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(bin.Value.GetType()))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(0))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(nameLength))
	cmd.dataOffset++
	cmd.dataOffset += nameLength + valueLength

	return nil
}

func (cmd *baseCommand) writeOperationForOperation(operation *Operation) error {
	nameLength := copy(cmd.dataBuffer[(cmd.dataOffset+int(_OPERATION_HEADER_SIZE)):], operation.BinName)

	// check for float support
	cmd.checkServerCompatibility(operation.BinValue)

	valueLength, err := operation.BinValue.write(cmd.dataBuffer, cmd.dataOffset+int(_OPERATION_HEADER_SIZE)+nameLength)
	if err != nil {
		return err
	}

	Buffer.Int32ToBytes(int32(nameLength+valueLength+4), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 4
	cmd.dataBuffer[cmd.dataOffset] = (byte(operation.OpType))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(operation.BinValue.GetType()))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(0))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(nameLength))
	cmd.dataOffset++
	cmd.dataOffset += nameLength + valueLength
	return nil
}

func (cmd *baseCommand) writeOperationForBinName(name string, operation OperationType) {
	nameLength := copy(cmd.dataBuffer[(cmd.dataOffset+int(_OPERATION_HEADER_SIZE)):], name)
	Buffer.Int32ToBytes(int32(nameLength+4), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 4
	cmd.dataBuffer[cmd.dataOffset] = (byte(operation))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(0))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(0))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(nameLength))
	cmd.dataOffset++
	cmd.dataOffset += nameLength
}

func (cmd *baseCommand) writeOperationForOperationType(operation OperationType) {
	Buffer.Int32ToBytes(int32(4), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 4
	cmd.dataBuffer[cmd.dataOffset] = (byte(operation))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (0)
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (0)
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (0)
	cmd.dataOffset++
}

func (cmd *baseCommand) checkServerCompatibility(val Value) {
	// check for float support
	switch val.GetType() {
	case ParticleType.FLOAT:
		if !cmd.node.supportsFloat.Get() {
			panic("This cluster node doesn't support double precision floating-point values.")
		}
	case ParticleType.GEOJSON:
		if !cmd.node.supportsGeo.Get() {
			panic("This cluster node doesn't support geo-spatial features.")
		}
	}
}

func (cmd *baseCommand) writeFieldValue(value Value, ftype FieldType) {
	offset := cmd.dataOffset + int(_FIELD_HEADER_SIZE)
	cmd.dataBuffer[offset] = byte(value.GetType())

	// check for float support
	cmd.checkServerCompatibility(value)

	offset++
	len, _ := value.write(cmd.dataBuffer, offset)
	len++
	cmd.writeFieldHeader(len, ftype)
	cmd.dataOffset += len
}

func (cmd *baseCommand) writeFieldString(str string, ftype FieldType) {
	len := copy(cmd.dataBuffer[(cmd.dataOffset+int(_FIELD_HEADER_SIZE)):], str)
	cmd.writeFieldHeader(len, ftype)
	cmd.dataOffset += len
}

func (cmd *baseCommand) writeFieldBytes(bytes []byte, ftype FieldType) {
	copy(cmd.dataBuffer[cmd.dataOffset+int(_FIELD_HEADER_SIZE):], bytes)

	cmd.writeFieldHeader(len(bytes), ftype)
	cmd.dataOffset += len(bytes)
}

func (cmd *baseCommand) writeFieldHeader(size int, ftype FieldType) {
	Buffer.Int32ToBytes(int32(size+1), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 4
	cmd.dataBuffer[cmd.dataOffset] = (byte(ftype))
	cmd.dataOffset++
}

func (cmd *baseCommand) begin() {
	cmd.dataOffset = int(_MSG_TOTAL_HEADER_SIZE)
}

func (cmd *baseCommand) sizeBuffer() error {
	return cmd.sizeBufferSz(cmd.dataOffset)
}

var (
	// MaxBufferSize protects against allocating massive memory blocks
	// for buffers. Tweak this number if you are returning a lot of
	// LDT elements in your queries.
	MaxBufferSize = 1024 * 1024 * 10 // 10 MB
)

func (cmd *baseCommand) sizeBufferSz(size int) error {
	// Corrupted data streams can result in a huge length.
	// Do a sanity check here.
	if size > MaxBufferSize {
		return NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Invalid size for buffer: %d", size))
	}

	if size <= len(cmd.dataBuffer) {
		// don't touch the buffer
	} else if size <= cap(cmd.dataBuffer) {
		cmd.dataBuffer = cmd.dataBuffer[:size]
	} else {
		// not enough space
		cmd.dataBuffer = make([]byte, size)
	}

	return nil
}

func (cmd *baseCommand) end() {
	var size = int64(cmd.dataOffset-8) | (_CL_MSG_VERSION << 56) | (_AS_MSG_TYPE << 48)
	Buffer.Int64ToBytes(size, cmd.dataBuffer, 0)
}

////////////////////////////////////

// a custom buffer pool with fine grained control over its contents
// maxSize: 128KiB
// initial bufferSize: 16 KiB
// maximum buffer size to keep in the pool: 128K
var bufPool = NewBufferPool(512, 16*1024, 128*1024)

// SetCommandBufferPool can be used to customize the command Buffer Pool parameters to calibrate
// the pool for different workloads
func SetCommandBufferPool(poolSize, initBufSize, maxBufferSize int) {
	bufPool = NewBufferPool(poolSize, initBufSize, maxBufferSize)
}

func (cmd *baseCommand) execute(ifc command) (err error) {
	policy := ifc.getPolicy(ifc).GetBasePolicy()
	iterations := 0

	// set timeout outside the loop
	limit := time.Now().Add(policy.Timeout)

	// Execute command until successful, timed out or maximum iterations have been reached.
	for {
		// too many retries
		if iterations++; (policy.MaxRetries > 0) && (iterations > policy.MaxRetries+1) {
			return NewAerospikeError(TIMEOUT, "command execution timed out: Exceeded number of retries. See `Policy.MaxRetries`")
		}

		// Sleep before trying again, after the first iteration
		if iterations > 1 && policy.SleepBetweenRetries > 0 {
			time.Sleep(policy.SleepBetweenRetries)
		}

		// check for command timeout
		if policy.Timeout > 0 && time.Now().After(limit) {
			break
		}

		node, err := ifc.getNode(ifc)
		if err != nil {
			// Node is currently inactive.  Retry.
			continue
		}

		// set command node, so when you return a record it has the node
		cmd.node = node

		cmd.conn, err = node.GetConnection(policy.Timeout)
		if err != nil {
			// Socket connection error has occurred. Decrease health and retry.
			node.DecreaseHealth()

			Logger.Warn("Node " + node.String() + ": " + err.Error())
			continue
		}

		// Draw a buffer from buffer pool, and make sure it will be put back
		cmd.dataBuffer = bufPool.Get()
		// defer bufPool.Put(cmd.dataBuffer)

		// Set command buffer.
		err = ifc.writeBuffer(ifc)
		if err != nil {
			// All runtime exceptions are considered fatal. Do not retry.
			// Close socket to flush out possible garbage. Do not put back in pool.
			node.InvalidateConnection(cmd.conn)
			return err
		}

		// Reset timeout in send buffer (destined for server) and socket.
		Buffer.Int32ToBytes(int32(policy.Timeout/time.Millisecond), cmd.dataBuffer, 22)

		// Send command.
		_, err = cmd.conn.Write(cmd.dataBuffer[:cmd.dataOffset])
		if err != nil {
			// IO errors are considered temporary anomalies. Retry.
			// Close socket to flush out possible garbage. Do not put back in pool.
			node.InvalidateConnection(cmd.conn)

			Logger.Warn("Node " + node.String() + ": " + err.Error())
			// IO error means connection to server node is unhealthy.
			// Reflect cmd status.
			node.DecreaseHealth()
			continue
		}

		// Parse results.
		err = ifc.parseResult(ifc, cmd.conn)
		if err != nil {
			// close the connection
			// cancelling/closing the batch/multi commands will return an error, which will
			// close the connection to throw away its data and signal the server about the
			// situation. We will not put back the connection in the buffer.
			if KeepConnection(err) {
				// Put connection back in pool.
				node.PutConnection(cmd.conn)
			} else {
				node.InvalidateConnection(cmd.conn)
			}
			return err
		}

		// Reflect healthy status.
		node.RestoreHealth()

		// Put connection back in pool.
		node.PutConnection(cmd.conn)

		// put back buffer to the pool
		bufPool.Put(cmd.dataBuffer)

		// command has completed successfully.  Exit method.
		return nil

	}

	// execution timeout
	return NewAerospikeError(TIMEOUT, "command execution timed out: See `Policy.Timeout`")
}

func (cmd *baseCommand) parseRecordResults(ifc command, receiveSize int) (bool, error) {
	panic(errors.New("Abstract method. Should not end up here"))
}

func (cmd *baseCommand) setConnection(conn *Connection) {
	cmd.conn = conn
}

func (cmd *baseCommand) getConnection() *Connection {
	return cmd.conn
}
