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
	"strconv"
	"strings"

	. "github.com/aerospike/aerospike-client-go/types"
)

// ExecuteTask is used to poll for long running server execute job completion.
type ExecuteTask struct {
	*BaseTask

	taskId uint64
	scan   bool
}

// NewExecuteTask initializes task with fields needed to query server nodes.
func NewExecuteTask(cluster *Cluster, statement *Statement) *ExecuteTask {
	return &ExecuteTask{
		BaseTask: NewTask(cluster, false),
		taskId:   statement.TaskId,
		scan:     statement.IsScan(),
	}
}

// IsDone queries all nodes for task completion status.
func (etsk *ExecuteTask) IsDone() (bool, error) {
	var command string
	if etsk.scan {
		command = "scan-list"
	} else {
		command = "query-list"
	}

	nodes := etsk.cluster.GetNodes()
	done := false

	for _, node := range nodes {
		conn, err := node.GetConnection(0)
		if err != nil {
			return false, err
		}
		responseMap, err := RequestInfo(conn, command)
		if err != nil {
			node.InvalidateConnection(conn)
			return false, err
		}

		node.PutConnection(conn)

		response := responseMap[command]
		find := "job_id=" + strconv.FormatUint(etsk.taskId, 10) + ":"
		index := strings.Index(response, find)

		if index < 0 {
			done = true
			continue
		}

		begin := index + len(find)
		response = response[begin:]
		find = "job_status="
		index = strings.Index(response, find)

		if index < 0 {
			continue
		}

		begin = index + len(find)
		response = response[begin:]
		end := strings.Index(response, ":")
		status := response[:end]

		if status == "ABORTED" {
			return false, NewAerospikeError(QUERY_TERMINATED)
		} else if status == "IN PROGRESS" {
			return false, nil
		} else if status == "DONE" {
			done = true
		}
	}

	return done, nil
}

// OnComplete returns a channel which will be closed when the task is
// completed.
// If an error is encountered while performing the task, an error
// will be sent on the channel.
func (etsk *ExecuteTask) OnComplete() chan error {
	return etsk.onComplete(etsk)
}
