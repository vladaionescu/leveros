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
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
	"sync"

	lualib "github.com/aerospike/aerospike-client-go/internal/lua"
	. "github.com/aerospike/aerospike-client-go/types"
	"github.com/yuin/gopher-lua"
)

// Client encapsulates an Aerospike cluster.
// All database operations are available against this object.
type Client struct {
	cluster *Cluster

	// DefaultPolicy is used for all read commands without a specific policy.
	DefaultPolicy *BasePolicy
	// DefaultWritePolicy is used for all write commands without a specific policy.
	DefaultWritePolicy *WritePolicy
	// DefaultScanPolicy is used for all query commands without a specific policy.
	DefaultScanPolicy *ScanPolicy
	// DefaultQueryPolicy is used for all scan commands without a specific policy.
	DefaultQueryPolicy *QueryPolicy
	// DefaultAdminPolicy is used for all security commands without a specific policy.
	DefaultAdminPolicy *AdminPolicy
}

//-------------------------------------------------------
// Constructors
//-------------------------------------------------------

// NewClient generates a new Client instance.
func NewClient(hostname string, port int) (*Client, error) {
	return NewClientWithPolicyAndHost(NewClientPolicy(), NewHost(hostname, port))
}

// NewClientWithPolicy generates a new Client using the specified ClientPolicy.
// If the policy is nil, the default relevant policy will be used.
func NewClientWithPolicy(policy *ClientPolicy, hostname string, port int) (*Client, error) {
	return NewClientWithPolicyAndHost(policy, NewHost(hostname, port))
}

// NewClientWithPolicyAndHost generates a new Client the specified ClientPolicy and
// sets up the cluster using the provided hosts.
// If the policy is nil, the default relevant policy will be used.
func NewClientWithPolicyAndHost(policy *ClientPolicy, hosts ...*Host) (*Client, error) {
	if policy == nil {
		policy = NewClientPolicy()
	}

	cluster, err := NewCluster(policy, hosts)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to host(s): %v", hosts)
	}
	return &Client{
		cluster:            cluster,
		DefaultPolicy:      NewPolicy(),
		DefaultWritePolicy: NewWritePolicy(0, 0),
		DefaultScanPolicy:  NewScanPolicy(),
		DefaultQueryPolicy: NewQueryPolicy(),
		DefaultAdminPolicy: NewAdminPolicy(),
	}, nil

}

//-------------------------------------------------------
// Cluster Connection Management
//-------------------------------------------------------

// Close closes all client connections to database server nodes.
func (clnt *Client) Close() {
	clnt.cluster.Close()
}

// IsConnected determines if the client is ready to talk to the database server cluster.
func (clnt *Client) IsConnected() bool {
	return clnt.cluster.IsConnected()
}

// GetNodes returns an array of active server nodes in the cluster.
func (clnt *Client) GetNodes() []*Node {
	return clnt.cluster.GetNodes()
}

// GetNodeNames returns a list of active server node names in the cluster.
func (clnt *Client) GetNodeNames() []string {
	nodes := clnt.cluster.GetNodes()
	names := make([]string, 0, len(nodes))

	for _, node := range nodes {
		names = append(names, node.GetName())
	}
	return names
}

//-------------------------------------------------------
// Write Record Operations
//-------------------------------------------------------

// Put writes record bin(s) to the server.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Put(policy *WritePolicy, key *Key, binMap BinMap) error {
	// get a slice of pre-allocated and pooled bins
	bins := binPool.Get(len(binMap)).([]*Bin)
	res := clnt.PutBins(policy, key, binMapToBins(bins[:len(binMap)], binMap)...)
	binPool.Put(bins)
	return res
}

// PutBins writes record bin(s) to the server.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This method avoids using the BinMap allocation and iteration and is lighter on GC.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) PutBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	policy = clnt.getUsableWritePolicy(policy)
	command := newWriteCommand(clnt.cluster, policy, key, bins, WRITE)
	return command.Execute()
}

// PutObject writes record bin(s) to the server.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) PutObject(policy *WritePolicy, key *Key, obj interface{}) (err error) {
	policy = clnt.getUsableWritePolicy(policy)

	bins := marshal(obj, clnt.cluster.supportsFloat.Get())
	command := newWriteCommand(clnt.cluster, policy, key, bins, WRITE)
	res := command.Execute()
	binPool.Put(bins)
	return res
}

//-------------------------------------------------------
// Operations string
//-------------------------------------------------------

// Append appends bin value's string to existing record bin values.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This call only works for string and []byte values.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Append(policy *WritePolicy, key *Key, binMap BinMap) error {
	// get a slice of pre-allocated and pooled bins
	bins := binPool.Get(len(binMap)).([]*Bin)
	res := clnt.AppendBins(policy, key, binMapToBins(bins[:len(binMap)], binMap)...)
	binPool.Put(bins)
	return res
}

// AppendBins works the same as Append, but avoids BinMap allocation and iteration.
func (clnt *Client) AppendBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	policy = clnt.getUsableWritePolicy(policy)
	command := newWriteCommand(clnt.cluster, policy, key, bins, APPEND)
	return command.Execute()
}

// Prepend prepends bin value's string to existing record bin values.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This call works only for string and []byte values.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Prepend(policy *WritePolicy, key *Key, binMap BinMap) error {
	bins := binPool.Get(len(binMap)).([]*Bin)
	res := clnt.PrependBins(policy, key, binMapToBins(bins[:len(binMap)], binMap)...)
	binPool.Put(bins)
	return res
}

// PrependBins works the same as Prepend, but avoids BinMap allocation and iteration.
func (clnt *Client) PrependBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	policy = clnt.getUsableWritePolicy(policy)
	command := newWriteCommand(clnt.cluster, policy, key, bins, PREPEND)
	return command.Execute()
}

//-------------------------------------------------------
// Arithmetic Operations
//-------------------------------------------------------

// Add adds integer bin values to existing record bin values.
// The policy specifies the transaction timeout, record expiration and how the transaction is
// handled when the record already exists.
// This call only works for integer values.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Add(policy *WritePolicy, key *Key, binMap BinMap) error {
	// get a slice of pre-allocated and pooled bins
	bins := binPool.Get(len(binMap)).([]*Bin)
	res := clnt.AddBins(policy, key, binMapToBins(bins[:len(binMap)], binMap)...)
	binPool.Put(bins)
	return res
}

// AddBins works the same as Add, but avoids BinMap allocation and iteration.
func (clnt *Client) AddBins(policy *WritePolicy, key *Key, bins ...*Bin) error {
	policy = clnt.getUsableWritePolicy(policy)
	command := newWriteCommand(clnt.cluster, policy, key, bins, ADD)
	return command.Execute()
}

//-------------------------------------------------------
// Delete Operations
//-------------------------------------------------------

// Delete deletes a record for specified key.
// The policy specifies the transaction timeout.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Delete(policy *WritePolicy, key *Key) (bool, error) {
	policy = clnt.getUsableWritePolicy(policy)
	command := newDeleteCommand(clnt.cluster, policy, key)
	err := command.Execute()
	return command.Existed(), err
}

//-------------------------------------------------------
// Touch Operations
//-------------------------------------------------------

// Touch updates a record's metadata.
// If the record exists, the record's TTL will be reset to the
// policy's expiration.
// If the record doesn't exist, it will return an error.
func (clnt *Client) Touch(policy *WritePolicy, key *Key) error {
	policy = clnt.getUsableWritePolicy(policy)
	command := newTouchCommand(clnt.cluster, policy, key)
	return command.Execute()
}

//-------------------------------------------------------
// Existence-Check Operations
//-------------------------------------------------------

// Exists determine if a record key exists.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Exists(policy *BasePolicy, key *Key) (bool, error) {
	policy = clnt.getUsablePolicy(policy)
	command := newExistsCommand(clnt.cluster, policy, key)
	err := command.Execute()
	return command.Exists(), err
}

// BatchExists determines if multiple record keys exist in one batch request.
// The returned boolean array is in positional order with the original key array order.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) BatchExists(policy *BasePolicy, keys []*Key) ([]bool, error) {
	policy = clnt.getUsablePolicy(policy)

	// same array can be used without synchronization;
	// when a key exists, the corresponding index will be marked true
	existsArray := make([]bool, len(keys))

	if err := clnt.batchExecute(keys, func(node *Node, bns *batchNamespace) command {
		return newBatchCommandExists(node, bns, policy, keys, existsArray)
	}); err != nil {
		return nil, err
	}

	return existsArray, nil
}

//-------------------------------------------------------
// Read Record Operations
//-------------------------------------------------------

// Get reads a record header and bins for specified key.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Get(policy *BasePolicy, key *Key, binNames ...string) (*Record, error) {
	policy = clnt.getUsablePolicy(policy)

	command := newReadCommand(clnt.cluster, policy, key, binNames)
	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

// GetObject reads a record for specified key and puts the result into the provided object.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) GetObject(policy *BasePolicy, key *Key, obj interface{}) error {
	policy = clnt.getUsablePolicy(policy)

	rval := reflect.ValueOf(obj)
	cacheObjectTags(rval)
	binNames := objectMappings.getFields(rval)

	command := newReadCommand(clnt.cluster, policy, key, binNames)
	command.object = obj
	if err := command.Execute(); err != nil {
		return err
	}
	return nil
}

// GetHeader reads a record generation and expiration only for specified key.
// Bins are not read.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) GetHeader(policy *BasePolicy, key *Key) (*Record, error) {
	policy = clnt.getUsablePolicy(policy)

	command := newReadHeaderCommand(clnt.cluster, policy, key)
	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

//-------------------------------------------------------
// Batch Read Operations
//-------------------------------------------------------

// BatchGet reads multiple record headers and bins for specified keys in one batch request.
// The returned records are in positional order with the original key array order.
// If a key is not found, the positional record will be nil.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) BatchGet(policy *BasePolicy, keys []*Key, binNames ...string) ([]*Record, error) {
	policy = clnt.getUsablePolicy(policy)

	// same array can be used without synchronization;
	// when a key exists, the corresponding index will be set to record
	records := make([]*Record, len(keys))

	binSet := map[string]struct{}{}
	for idx := range binNames {
		binSet[binNames[idx]] = struct{}{}
	}

	err := clnt.batchExecute(keys, func(node *Node, bns *batchNamespace) command {
		return newBatchCommandGet(node, bns, policy, keys, binSet, records, _INFO1_READ)
	})
	if err != nil {
		return nil, err
	}

	return records, nil
}

// BatchGetHeader reads multiple record header data for specified keys in one batch request.
// The returned records are in positional order with the original key array order.
// If a key is not found, the positional record will be nil.
// The policy can be used to specify timeouts.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) BatchGetHeader(policy *BasePolicy, keys []*Key) ([]*Record, error) {
	policy = clnt.getUsablePolicy(policy)

	// same array can be used without synchronization;
	// when a key exists, the corresponding index will be set to record
	records := make([]*Record, len(keys))

	err := clnt.batchExecute(keys, func(node *Node, bns *batchNamespace) command {
		return newBatchCommandGet(node, bns, policy, keys, nil, records, _INFO1_READ|_INFO1_NOBINDATA)
	})
	if err != nil {
		return nil, err
	}

	return records, nil
}

//-------------------------------------------------------
// Generic Database Operations
//-------------------------------------------------------

// Operate performs multiple read/write operations on a single key in one batch request.
// An example would be to add an integer value to an existing record and then
// read the result, all in one database call.
//
// Write operations are always performed first, regardless of operation order
// relative to read operations.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Operate(policy *WritePolicy, key *Key, operations ...*Operation) (*Record, error) {
	policy = clnt.getUsableWritePolicy(policy)
	command := newOperateCommand(clnt.cluster, policy, key, operations)
	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}

//-------------------------------------------------------
// Scan Operations
//-------------------------------------------------------

// ScanAll reads all records in specified namespace and set from all nodes.
// If the policy's concurrentNodes is specified, each server node will be read in
// parallel. Otherwise, server nodes are read sequentially.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ScanAll(apolicy *ScanPolicy, namespace string, setName string, binNames ...string) (*Recordset, error) {
	policy := *clnt.getUsableScanPolicy(apolicy)

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
	}

	if policy.WaitUntilMigrationsAreOver {
		// wait until all migrations are finished
		if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// result recordset
	res := newRecordset(policy.RecordQueueSize, len(nodes))

	// the whole call should be wrapped in a goroutine
	if policy.ConcurrentNodes {
		for _, node := range nodes {
			go func(node *Node) {
				if err := clnt.scanNode(&policy, node, res, namespace, setName, binNames...); err != nil {
					res.sendError(err)
				}
			}(node)
		}
	} else {
		// scan nodes one by one
		go func() {
			for _, node := range nodes {
				if err := clnt.scanNode(&policy, node, res, namespace, setName, binNames...); err != nil {
					res.sendError(err)
					continue
				}
			}
		}()
	}

	return res, nil
}

// ScanNode reads all records in specified namespace and set for one node only.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ScanNode(apolicy *ScanPolicy, node *Node, namespace string, setName string, binNames ...string) (*Recordset, error) {
	policy := *clnt.getUsableScanPolicy(apolicy)

	// results channel must be async for performance
	res := newRecordset(policy.RecordQueueSize, 1)

	go clnt.scanNode(&policy, node, res, namespace, setName, binNames...)
	return res, nil
}

// ScanNode reads all records in specified namespace and set for one node only.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) scanNode(policy *ScanPolicy, node *Node, recordset *Recordset, namespace string, setName string, binNames ...string) error {
	if policy.WaitUntilMigrationsAreOver {
		// wait until migrations on node are finished
		if err := node.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			recordset.signalEnd()
			return err
		}
	}

	command := newScanCommand(node, policy, namespace, setName, binNames, recordset)
	return command.Execute()
}

// ScanAllObjects reads all records in specified namespace and set from all nodes.
// If the policy's concurrentNodes is specified, each server node will be read in
// parallel. Otherwise, server nodes are read sequentially.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ScanAllObjects(apolicy *ScanPolicy, objChan interface{}, namespace string, setName string, binNames ...string) (*Recordset, error) {
	policy := *clnt.getUsableScanPolicy(apolicy)

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
	}

	if policy.WaitUntilMigrationsAreOver {
		// wait until all migrations are finished
		if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// result recordset
	os := newObjectset(reflect.ValueOf(objChan), len(nodes))
	res := &Recordset{
		objectset: *os,
	}

	// the whole call should be wrapped in a goroutine
	if policy.ConcurrentNodes {
		for _, node := range nodes {
			go func(node *Node) {
				if err := clnt.scanNodeObjects(&policy, node, res, namespace, setName, binNames...); err != nil {
					res.sendError(err)
				}
			}(node)
		}
	} else {
		// scan nodes one by one
		go func() {
			for _, node := range nodes {
				if err := clnt.scanNodeObjects(&policy, node, res, namespace, setName, binNames...); err != nil {
					res.sendError(err)
					continue
				}
			}
		}()
	}

	return res, nil
}

// scanNodeObjects reads all records in specified namespace and set for one node only,
// and marshalls the results into the objects of the provided channel in Recordset.
// If the policy is nil, the default relevant policy will be used.
// The resulting records will be marshalled into the objChan.
// objChan will be closed after all the records are read.
func (clnt *Client) ScanNodeObjects(apolicy *ScanPolicy, node *Node, objChan interface{}, namespace string, setName string, binNames ...string) (*Recordset, error) {
	policy := *clnt.getUsableScanPolicy(apolicy)

	// results channel must be async for performance
	os := newObjectset(reflect.ValueOf(objChan), 1)
	res := &Recordset{
		objectset: *os,
	}

	go clnt.scanNodeObjects(&policy, node, res, namespace, setName, binNames...)
	return res, nil
}

// scanNodeObjects reads all records in specified namespace and set for one node only,
// and marshalls the results into the objects of the provided channel in Recordset.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) scanNodeObjects(policy *ScanPolicy, node *Node, recordset *Recordset, namespace string, setName string, binNames ...string) error {
	if policy.WaitUntilMigrationsAreOver {
		// wait until migrations on node are finished
		if err := node.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			recordset.signalEnd()
			return err
		}
	}

	command := newScanObjectsCommand(node, policy, namespace, setName, binNames, recordset)
	return command.Execute()
}

//-------------------------------------------------------------------
// Large collection functions (Supported by Aerospike 3 servers only)
//-------------------------------------------------------------------

// GetLargeList initializes large list operator.
// This operator can be used to create and manage a list
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) GetLargeList(policy *WritePolicy, key *Key, binName string, userModule string) *LargeList {
	policy = clnt.getUsableWritePolicy(policy)
	return NewLargeList(clnt, policy, key, binName, userModule)
}

// GetLargeMap initializes a large map operator.
// This operator can be used to create and manage a map
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
// NOTICE: DEPRECATED ON SERVER. Will be removed in future.
func (clnt *Client) GetLargeMap(policy *WritePolicy, key *Key, binName string, userModule string) *LargeMap {
	policy = clnt.getUsableWritePolicy(policy)
	return NewLargeMap(clnt, policy, key, binName, userModule)
}

// GetLargeSet initializes large set operator.
// This operator can be used to create and manage a set
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
// NOTICE: DEPRECATED ON SERVER. Will be removed in future.
func (clnt *Client) GetLargeSet(policy *WritePolicy, key *Key, binName string, userModule string) *LargeSet {
	policy = clnt.getUsableWritePolicy(policy)
	return NewLargeSet(clnt, policy, key, binName, userModule)
}

// GetLargeStack initializes large stack operator.
// This operator can be used to create and manage a stack
// within a single bin.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
// NOTICE: DEPRECATED ON SERVER. Will be removed in future.
func (clnt *Client) GetLargeStack(policy *WritePolicy, key *Key, binName string, userModule string) *LargeStack {
	policy = clnt.getUsableWritePolicy(policy)
	return NewLargeStack(clnt, policy, key, binName, userModule)
}

//---------------------------------------------------------------
// User defined functions (Supported by Aerospike 3 servers only)
//---------------------------------------------------------------

// RegisterUDFFromFile reads a file from file system and registers
// the containing a package user defined functions with the server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RegisterTask instance.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) RegisterUDFFromFile(policy *WritePolicy, clientPath string, serverPath string, language Language) (*RegisterTask, error) {
	policy = clnt.getUsableWritePolicy(policy)
	udfBody, err := ioutil.ReadFile(clientPath)
	if err != nil {
		return nil, err
	}

	return clnt.RegisterUDF(policy, udfBody, serverPath, language)
}

// RegisterUDF registers a package containing user defined functions with server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RegisterTask instance.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) RegisterUDF(policy *WritePolicy, udfBody []byte, serverPath string, language Language) (*RegisterTask, error) {
	policy = clnt.getUsableWritePolicy(policy)
	content := base64.StdEncoding.EncodeToString(udfBody)

	var strCmd bytes.Buffer
	// errors are to remove errcheck warnings
	// they will always be nil as stated in golang docs
	_, err := strCmd.WriteString("udf-put:filename=")
	_, err = strCmd.WriteString(serverPath)
	_, err = strCmd.WriteString(";content=")
	_, err = strCmd.WriteString(content)
	_, err = strCmd.WriteString(";content-len=")
	_, err = strCmd.WriteString(strconv.Itoa(len(content)))
	_, err = strCmd.WriteString(";udf-type=")
	_, err = strCmd.WriteString(string(language))
	_, err = strCmd.WriteString(";")

	// Send UDF to one node. That node will distribute the UDF to other nodes.
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	conn, err := node.GetConnection(policy.Timeout)
	if err != nil {
		return nil, err
	}

	responseMap, err := RequestInfo(conn, strCmd.String())
	if err != nil {
		node.InvalidateConnection(conn)
		return nil, err
	}
	node.PutConnection(conn)

	var response string
	for _, v := range responseMap {
		if strings.Trim(v, " ") != "" {
			response = v
		}
	}

	res := make(map[string]string)
	vals := strings.Split(response, ";")
	for _, pair := range vals {
		t := strings.SplitN(pair, "=", 2)
		if len(t) == 2 {
			res[t[0]] = t[1]
		} else if len(t) == 1 {
			res[t[0]] = ""
		}
	}

	if _, exists := res["error"]; exists {
		msg, _ := base64.StdEncoding.DecodeString(res["message"])
		return nil, NewAerospikeError(COMMAND_REJECTED, fmt.Sprintf("Registration failed: %s\nFile: %s\nLine: %s\nMessage: %s",
			res["error"], res["file"], res["line"], msg))
	}
	return NewRegisterTask(clnt.cluster, serverPath), nil
}

// RemoveUDF removes a package containing user defined functions in the server.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// RemoveTask instance.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) RemoveUDF(policy *WritePolicy, udfName string) (*RemoveTask, error) {
	policy = clnt.getUsableWritePolicy(policy)
	var strCmd bytes.Buffer
	// errors are to remove errcheck warnings
	// they will always be nil as stated in golang docs
	_, err := strCmd.WriteString("udf-remove:filename=")
	_, err = strCmd.WriteString(udfName)
	_, err = strCmd.WriteString(";")

	// Send command to one node. That node will distribute it to other nodes.
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	conn, err := node.GetConnection(policy.Timeout)
	if err != nil {
		return nil, err
	}

	responseMap, err := RequestInfo(conn, strCmd.String())
	if err != nil {
		node.InvalidateConnection(conn)
		return nil, err
	}
	node.PutConnection(conn)

	var response string
	for _, v := range responseMap {
		if strings.Trim(v, " ") != "" {
			response = v
		}
	}

	if response == "ok" {
		return NewRemoveTask(clnt.cluster, udfName), nil
	}
	return nil, NewAerospikeError(SERVER_ERROR, response)
}

// ListUDF lists all packages containing user defined functions in the server.
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ListUDF(policy *BasePolicy) ([]*UDF, error) {
	policy = clnt.getUsablePolicy(policy)

	var strCmd bytes.Buffer
	// errors are to remove errcheck warnings
	// they will always be nil as stated in golang docs
	_, err := strCmd.WriteString("udf-list")

	// Send command to one node. That node will distribute it to other nodes.
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	conn, err := node.GetConnection(policy.Timeout)
	if err != nil {
		return nil, err
	}

	responseMap, err := RequestInfo(conn, strCmd.String())
	if err != nil {
		node.InvalidateConnection(conn)
		return nil, err
	}
	node.PutConnection(conn)

	var response string
	for _, v := range responseMap {
		if strings.Trim(v, " ") != "" {
			response = v
		}
	}

	vals := strings.Split(response, ";")
	res := make([]*UDF, 0, len(vals))

	for _, udfInfo := range vals {
		if strings.Trim(udfInfo, " ") == "" {
			continue
		}
		udfParts := strings.Split(udfInfo, ",")

		udf := &UDF{}
		for _, values := range udfParts {
			valueParts := strings.Split(values, "=")
			if len(valueParts) == 2 {
				switch valueParts[0] {
				case "filename":
					udf.Filename = valueParts[1]
				case "hash":
					udf.Hash = valueParts[1]
				case "type":
					udf.Language = Language(valueParts[1])
				}
			}
		}
		res = append(res, udf)
	}

	return res, nil
}

// Execute executes a user defined function on server and return results.
// The function operates on a single record.
// The package name is used to locate the udf file location:
//
// udf file = <server udf dir>/<package name>.lua
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Execute(policy *WritePolicy, key *Key, packageName string, functionName string, args ...Value) (interface{}, error) {
	policy = clnt.getUsableWritePolicy(policy)
	command := newExecuteCommand(clnt.cluster, policy, key, packageName, functionName, args)
	if err := command.Execute(); err != nil {
		return nil, err
	}

	record := command.GetRecord()

	if record == nil || len(record.Bins) == 0 {
		return nil, nil
	}

	resultMap := record.Bins

	// User defined functions don't have to return a value.
	if exists, obj := mapContainsKeyPartial(resultMap, "SUCCESS"); exists {
		return obj, nil
	}

	if _, obj := mapContainsKeyPartial(resultMap, "FAILURE"); obj != nil {
		return nil, fmt.Errorf("%v", obj)
	}

	return nil, NewAerospikeError(UDF_BAD_RESPONSE, "Invalid UDF return value")
}

func mapContainsKeyPartial(theMap map[string]interface{}, key string) (bool, interface{}) {
	for k, v := range theMap {
		if strings.Index(k, key) >= 0 {
			return true, v
		}
	}
	return false, nil
}

//----------------------------------------------------------
// Query/Execute UDF (Supported by Aerospike 3 servers only)
//----------------------------------------------------------

// ExecuteUDF applies user defined function on records that match the statement filter.
// Records are not returned to the client.
// This asynchronous server call will return before command is complete.
// The user can optionally wait for command completion by using the returned
// ExecuteTask instance.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) ExecuteUDF(policy *QueryPolicy,
	statement *Statement,
	packageName string,
	functionName string,
	functionArgs ...Value,
) (*ExecuteTask, error) {
	policy = clnt.getUsableQueryPolicy(policy)

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "ExecuteUDF failed because cluster is empty.")
	}

	// wait until all migrations are finished
	if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
		return nil, err
	}

	statement.SetAggregateFunction(packageName, functionName, functionArgs, false)

	errs := []error{}
	for i := range nodes {
		command := newServerCommand(nodes[i], policy, statement)
		if err := command.Execute(); err != nil {
			errs = append(errs, err)
		}
	}

	return NewExecuteTask(clnt.cluster, statement), mergeErrors(errs)
}

//--------------------------------------------------------
// Query Aggregate functions (Supported by Aerospike 3 servers only)
//--------------------------------------------------------

// SetLuaPath sets the Lua interpreter path to files
// This path is used to load UDFs for QueryAggregate command
func SetLuaPath(lpath string) {
	lualib.SetPath(lpath)
}

// QueryAggregate executes a Map/Reduce query and returns the results.
// The query executor puts records on the channel from separate goroutines.
// The caller can concurrently pop records off the channel through the
// Recordset.Records channel.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) QueryAggregate(policy *QueryPolicy, statement *Statement, packageName, functionName string, functionArgs ...interface{}) (*Recordset, error) {
	statement.SetAggregateFunction(packageName, functionName, ToValueSlice(functionArgs), true)

	policy = clnt.getUsableQueryPolicy(policy)

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "QueryAggregate failed because cluster is empty.")
	}

	if policy.WaitUntilMigrationsAreOver {
		// wait until all migrations are finished
		if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// results channel must be async for performance
	recSet := newRecordset(policy.RecordQueueSize, len(nodes))

	// get a lua instance
	luaInstance := lualib.LuaPool.Get().(*lua.LState)
	if luaInstance == nil {
		return nil, fmt.Errorf("Error fetching a lua instance from pool")
	}

	// Input Channel
	inputChan := make(chan interface{}, 4096) // 4096 = number of partitions
	istream := lualib.NewLuaStream(luaInstance, inputChan)

	// Output Channe;
	outputChan := make(chan interface{})
	ostream := lualib.NewLuaStream(luaInstance, outputChan)

	// results channel must be async for performance
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, node := range nodes {
		// copy policies to avoid race conditions
		newPolicy := *policy
		command := newQueryAggregateCommand(node, &newPolicy, statement, recSet)
		command.luaInstance = luaInstance
		command.inputChan = inputChan

		go func() {
			defer wg.Done()
			err := command.Execute()
			if err != nil {
				recSet.sendError(err)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(inputChan)
	}()

	go func() {
		// we cannot signal end and close the recordset
		// while processing is still going on
		// We will do it only here, after all processing is over
		defer func() {
			for i := 0; i < len(nodes); i++ {
				recSet.signalEnd()
			}
		}()

		for val := range outputChan {
			recSet.Records <- &Record{Bins: BinMap{"SUCCESS": val}}
		}
	}()

	go func() {
		defer close(outputChan)
		defer luaInstance.Close()

		err := luaInstance.DoFile(lualib.LuaPath() + packageName + ".lua")
		if err != nil {
			recSet.Errors <- err
			return
		}

		fn := luaInstance.GetGlobal(functionName)

		luaArgs := []lua.LValue{fn, lualib.NewValue(luaInstance, 2), istream, ostream}
		for _, a := range functionArgs {
			luaArgs = append(luaArgs, lualib.NewValue(luaInstance, unwrapValue(a)))
		}

		if err := luaInstance.CallByParam(lua.P{
			Fn:      luaInstance.GetGlobal("apply_stream"),
			NRet:    1,
			Protect: true,
		},
			luaArgs...,
		); err != nil {
			recSet.Errors <- err
			return
		}

		luaInstance.Get(-1) // returned value
		luaInstance.Pop(1)  // remove received value
	}()

	return recSet, nil
}

//--------------------------------------------------------
// Query functions (Supported by Aerospike 3 servers only)
//--------------------------------------------------------

// Query executes a query and returns a Recordset.
// The query executor puts records on the channel from separate goroutines.
// The caller can concurrently pop records off the channel through the
// Recordset.Records channel.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) Query(policy *QueryPolicy, statement *Statement) (*Recordset, error) {
	policy = clnt.getUsableQueryPolicy(policy)

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "Query failed because cluster is empty.")
	}

	if policy.WaitUntilMigrationsAreOver {
		// wait until all migrations are finished
		if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// results channel must be async for performance
	recSet := newRecordset(policy.RecordQueueSize, len(nodes))

	// results channel must be async for performance
	for _, node := range nodes {
		// copy policies to avoid race conditions
		newPolicy := *policy
		command := newQueryRecordCommand(node, &newPolicy, statement, recSet)
		go func() {
			err := command.Execute()
			if err != nil {
				recSet.sendError(err)
			}
		}()
	}

	return recSet, nil
}

// QueryNode executes a query on a specific node and returns a recordset.
// The caller can concurrently pop records off the channel through the
// record channel.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) QueryNode(policy *QueryPolicy, node *Node, statement *Statement) (*Recordset, error) {
	policy = clnt.getUsableQueryPolicy(policy)

	if policy.WaitUntilMigrationsAreOver {
		// wait until all migrations are finished
		if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// results channel must be async for performance
	recSet := newRecordset(policy.RecordQueueSize, 1)

	// copy policies to avoid race conditions
	newPolicy := *policy
	command := newQueryRecordCommand(node, &newPolicy, statement, recSet)
	go func() {
		err := command.Execute()
		if err != nil {
			recSet.sendError(err)
		}
	}()

	return recSet, nil
}

// QueryNodeObjects executes a query on all nodes in the cluster and marshals the records into the given channel.
// The query executor puts records on the channel from separate goroutines.
// The caller can concurrently pop objects.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) QueryObjects(policy *QueryPolicy, statement *Statement, objChan interface{}) (*Recordset, error) {
	policy = clnt.getUsableQueryPolicy(policy)

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, NewAerospikeError(SERVER_NOT_AVAILABLE, "Query failed because cluster is empty.")
	}

	if policy.WaitUntilMigrationsAreOver {
		// wait until all migrations are finished
		if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// results channel must be async for performance
	os := newObjectset(reflect.ValueOf(objChan), len(nodes))
	recSet := &Recordset{
		objectset: *os,
	}

	// the whole call sho
	// results channel must be async for performance
	for _, node := range nodes {
		// copy policies to avoid race conditions
		newPolicy := *policy
		command := newQueryObjectsCommand(node, &newPolicy, statement, recSet)
		go func() {
			err := command.Execute()
			if err != nil {
				recSet.sendError(err)
			}
		}()
	}

	return recSet, nil
}

// QueryNodeObjects executes a query on a specific node and marshals the records into the given channel.
// The caller can concurrently pop records off the channel.
//
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) QueryNodeObjects(policy *QueryPolicy, node *Node, statement *Statement, objChan interface{}) (*Recordset, error) {
	policy = clnt.getUsableQueryPolicy(policy)

	if policy.WaitUntilMigrationsAreOver {
		// wait until all migrations are finished
		if err := clnt.cluster.WaitUntillMigrationIsFinished(policy.Timeout); err != nil {
			return nil, err
		}
	}

	// results channel must be async for performance
	os := newObjectset(reflect.ValueOf(objChan), 1)
	recSet := &Recordset{
		objectset: *os,
	}

	// copy policies to avoid race conditions
	newPolicy := *policy
	command := newQueryRecordCommand(node, &newPolicy, statement, recSet)
	go func() {
		err := command.Execute()
		if err != nil {
			recSet.sendError(err)
		}
	}()

	return recSet, nil
}

// CreateIndex creates a secondary index.
// This asynchronous server call will return before the command is complete.
// The user can optionally wait for command completion by using the returned
// IndexTask instance.
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) CreateIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
	binName string,
	indexType IndexType,
) (*IndexTask, error) {
	policy = clnt.getUsableWritePolicy(policy)
	return clnt.CreateComplexIndex(policy, namespace, setName, indexName, binName, indexType, ICT_DEFAULT)
}

// CreateComplexIndex creates a secondary index, with the ability to put indexes
// on bin containing complex data types, e.g: Maps and Lists.
// This asynchronous server call will return before the command is complete.
// The user can optionally wait for command completion by using the returned
// IndexTask instance.
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) CreateComplexIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
	binName string,
	indexType IndexType,
	indexCollectionType IndexCollectionType,
) (*IndexTask, error) {
	policy = clnt.getUsableWritePolicy(policy)

	var strCmd bytes.Buffer
	_, err := strCmd.WriteString("sindex-create:ns=")
	_, err = strCmd.WriteString(namespace)

	if len(setName) > 0 {
		_, err = strCmd.WriteString(";set=")
		_, err = strCmd.WriteString(setName)
	}

	_, err = strCmd.WriteString(";indexname=")
	_, err = strCmd.WriteString(indexName)
	_, err = strCmd.WriteString(";numbins=1")

	if indexCollectionType != ICT_DEFAULT {
		_, err = strCmd.WriteString(";indextype=")
		_, err = strCmd.WriteString(ictToString(indexCollectionType))
	}

	_, err = strCmd.WriteString(";indexdata=")
	_, err = strCmd.WriteString(binName)
	_, err = strCmd.WriteString(",")
	_, err = strCmd.WriteString(string(indexType))
	_, err = strCmd.WriteString(";priority=normal")

	// Send index command to one node. That node will distribute the command to other nodes.
	responseMap, err := clnt.sendInfoCommand(policy, strCmd.String())
	if err != nil {
		return nil, err
	}

	response := ""
	for _, v := range responseMap {
		response = v
	}

	if strings.ToUpper(response) == "OK" {
		// Return task that could optionally be polled for completion.
		return NewIndexTask(clnt.cluster, namespace, indexName), nil
	}

	if strings.HasPrefix(response, "FAIL:200") {
		// Index has already been created.  Do not need to poll for completion.
		return nil, NewAerospikeError(INDEX_FOUND)
	}

	return nil, NewAerospikeError(INDEX_GENERIC, "Create index failed: "+response)
}

// DropIndex deletes a secondary index.
// This method is only supported by Aerospike 3 servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) DropIndex(
	policy *WritePolicy,
	namespace string,
	setName string,
	indexName string,
) error {
	policy = clnt.getUsableWritePolicy(policy)
	var strCmd bytes.Buffer
	_, err := strCmd.WriteString("sindex-delete:ns=")
	_, err = strCmd.WriteString(namespace)

	if len(setName) > 0 {
		_, err = strCmd.WriteString(";set=")
		_, err = strCmd.WriteString(setName)
	}
	_, err = strCmd.WriteString(";indexname=")
	_, err = strCmd.WriteString(indexName)

	// Send index command to one node. That node will distribute the command to other nodes.
	responseMap, err := clnt.sendInfoCommand(policy, strCmd.String())
	if err != nil {
		return err
	}

	response := ""
	for _, v := range responseMap {
		response = v

		if strings.ToUpper(response) == "OK" {
			return nil
		}

		if strings.HasPrefix(response, "FAIL:201") {
			// Index did not previously exist. Return without error.
			return nil
		}
	}

	return NewAerospikeError(INDEX_GENERIC, "Drop index failed: "+response)
}

//-------------------------------------------------------
// User administration
//-------------------------------------------------------

// CreateUser creates a new user with password and roles. Clear-text password will be hashed using bcrypt
// before sending to server.
func (clnt *Client) CreateUser(policy *AdminPolicy, user string, password string, roles []string) error {
	policy = clnt.getUsableAdminPolicy(policy)

	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	command := newAdminCommand()
	return command.createUser(clnt.cluster, policy, user, hash, roles)
}

// DropUser removes a user from the cluster.
func (clnt *Client) DropUser(policy *AdminPolicy, user string) error {
	policy = clnt.getUsableAdminPolicy(policy)

	command := newAdminCommand()
	return command.dropUser(clnt.cluster, policy, user)
}

// ChangePassword changes a user's password. Clear-text password will be hashed using bcrypt before sending to server.
func (clnt *Client) ChangePassword(policy *AdminPolicy, user string, password string) error {
	policy = clnt.getUsableAdminPolicy(policy)

	if clnt.cluster.user == "" {
		return NewAerospikeError(INVALID_USER)
	}

	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	command := newAdminCommand()

	if user == clnt.cluster.user {
		// Change own password.
		if err := command.changePassword(clnt.cluster, policy, user, hash); err != nil {
			return err
		}
	} else {
		// Change other user's password by user admin.
		if err := command.setPassword(clnt.cluster, policy, user, hash); err != nil {
			return err
		}
	}

	clnt.cluster.changePassword(user, password, hash)

	return nil
}

// GrantRoles adds roles to user's list of roles.
func (clnt *Client) GrantRoles(policy *AdminPolicy, user string, roles []string) error {
	policy = clnt.getUsableAdminPolicy(policy)

	command := newAdminCommand()
	return command.grantRoles(clnt.cluster, policy, user, roles)
}

// RevokeRoles removes roles from user's list of roles.
func (clnt *Client) RevokeRoles(policy *AdminPolicy, user string, roles []string) error {
	policy = clnt.getUsableAdminPolicy(policy)

	command := newAdminCommand()
	return command.revokeRoles(clnt.cluster, policy, user, roles)
}

// QueryUser retrieves roles for a given user.
func (clnt *Client) QueryUser(policy *AdminPolicy, user string) (*UserRoles, error) {
	policy = clnt.getUsableAdminPolicy(policy)

	command := newAdminCommand()
	return command.queryUser(clnt.cluster, policy, user)
}

// QueryUsers retrieves all users and their roles.
func (clnt *Client) QueryUsers(policy *AdminPolicy) ([]*UserRoles, error) {
	policy = clnt.getUsableAdminPolicy(policy)

	command := newAdminCommand()
	return command.queryUsers(clnt.cluster, policy)
}

// String implements the Stringer interface for client
func (clnt *Client) String() string {
	if clnt.cluster != nil {
		return clnt.cluster.String()
	}
	return ""
}

//-------------------------------------------------------
// Internal Methods
//-------------------------------------------------------

func (clnt *Client) sendInfoCommand(policy *WritePolicy, command string) (map[string]string, error) {
	node, err := clnt.cluster.GetRandomNode()
	if err != nil {
		return nil, err
	}

	conn, err := node.GetConnection(policy.Timeout)
	if err != nil {
		return nil, err
	}

	info, err := newInfo(conn, command)
	if err != nil {
		node.InvalidateConnection(conn)
		return nil, err
	}
	node.PutConnection(conn)

	results, err := info.parseMultiResponse()
	if err != nil {
		return nil, err
	}

	return results, nil
}

// batchExecute Uses sync.WaitGroup to run commands using multiple goroutines,
// and waits for their return
func (clnt *Client) batchExecute(keys []*Key, cmdGen func(node *Node, bns *batchNamespace) command) error {

	batchNodes, err := newBatchNodeList(clnt.cluster, keys)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	// Use a goroutine per namespace per node
	errs := []error{}
	errm := new(sync.Mutex)

	wg.Add(len(batchNodes))
	for _, batchNode := range batchNodes {
		// copy to avoid race condition
		bn := *batchNode
		for _, bns := range bn.BatchNamespaces {
			go func(bn *Node, bns *batchNamespace) {
				defer wg.Done()
				command := cmdGen(bn, bns)
				if err := command.Execute(); err != nil {
					errm.Lock()
					errs = append(errs, err)
					errm.Unlock()
				}
			}(bn.Node, bns)
		}
	}

	wg.Wait()
	return mergeErrors(errs)
}

func (clnt *Client) getUsablePolicy(policy *BasePolicy) *BasePolicy {
	if policy == nil {
		if clnt.DefaultPolicy != nil {
			return clnt.DefaultPolicy
		}
		return NewPolicy()
	}
	return policy
}

func (clnt *Client) getUsableWritePolicy(policy *WritePolicy) *WritePolicy {
	if policy == nil {
		if clnt.DefaultWritePolicy != nil {
			return clnt.DefaultWritePolicy
		}
		return NewWritePolicy(0, 0)
	}
	return policy
}

func (clnt *Client) getUsableScanPolicy(policy *ScanPolicy) *ScanPolicy {
	if policy == nil {
		if clnt.DefaultScanPolicy != nil {
			return clnt.DefaultScanPolicy
		}
		return NewScanPolicy()
	}
	return policy
}

func (clnt *Client) getUsableQueryPolicy(policy *QueryPolicy) *QueryPolicy {
	if policy == nil {
		if clnt.DefaultQueryPolicy != nil {
			return clnt.DefaultQueryPolicy
		}
		return NewQueryPolicy()
	}
	return policy
}

func (clnt *Client) getUsableAdminPolicy(policy *AdminPolicy) *AdminPolicy {
	if policy == nil {
		if clnt.DefaultAdminPolicy != nil {
			return clnt.DefaultAdminPolicy
		}
		return NewAdminPolicy()
	}
	return policy
}

//-------------------------------------------------------
// Utility Functions
//-------------------------------------------------------

// mergeErrors merges several errors into one
func mergeErrors(errs []error) error {
	if errs == nil || len(errs) == 0 {
		return nil
	}
	var msg bytes.Buffer
	for _, err := range errs {
		msg.WriteString(err.Error())
		msg.WriteString("\n")
	}
	return errors.New(msg.String())
}
