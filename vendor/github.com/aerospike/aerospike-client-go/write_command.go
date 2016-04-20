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

// guarantee writeCommand implements command interface
var _ command = &writeCommand{}

type writeCommand struct {
	singleCommand

	policy    *WritePolicy
	bins      []*Bin
	operation OperationType
}

func newWriteCommand(cluster *Cluster,
	policy *WritePolicy,
	key *Key,
	bins []*Bin,
	operation OperationType) *writeCommand {

	newWriteCmd := &writeCommand{
		singleCommand: *newSingleCommand(cluster, key),
		policy:        policy,
		bins:          bins,
		operation:     operation,
	}

	return newWriteCmd
}

func (cmd *writeCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *writeCommand) writeBuffer(ifc command) error {
	return cmd.setWrite(cmd.policy, cmd.operation, cmd.key, cmd.bins)
}

func (cmd *writeCommand) parseResult(ifc command, conn *Connection) error {
	// Read header.
	if _, err := conn.Read(cmd.dataBuffer, int(_MSG_TOTAL_HEADER_SIZE)); err != nil {
		return err
	}

	resultCode := cmd.dataBuffer[13] & 0xFF

	if resultCode != 0 {
		return NewAerospikeError(ResultCode(resultCode))
	}
	if err := cmd.emptySocket(conn); err != nil {
		return err
	}
	return nil
}

func (cmd *writeCommand) Execute() error {
	return cmd.execute(cmd)
}
