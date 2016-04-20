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
	"hash"

	"github.com/aerospike/aerospike-client-go/pkg/ripemd160"
	. "github.com/aerospike/aerospike-client-go/types"
	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

// Key is the unique record identifier. Records can be identified using a specified namespace,
// an optional set name, and a user defined key which must be unique within a set.
// Records can also be identified by namespace/digest which is the combination used
// on the server.
type Key struct {
	// namespace. Equivalent to database name.
	namespace string

	// Optional set name. Equivalent to database table.
	setName string

	// Unique server hash value generated from set name and user key.
	digest []byte

	// Original user key. This key is immediately converted to a hash digest.
	// This key is not used or returned by the server by default. If the user key needs
	// to persist on the server, use one of the following methods:
	//
	// Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
	// and retrieved on multi-record scans and queries.
	// Explicitly store and retrieve the key in a bin.
	userKey Value
}

// Namespace returns key's namespace.
func (ky *Key) Namespace() string {
	return ky.namespace
}

// SetName returns key's set name.
func (ky *Key) SetName() string {
	return ky.setName
}

// Value returns key's value.
func (ky *Key) Value() Value {
	return ky.userKey
}

// Digest returns key digest.
func (ky *Key) Digest() []byte {
	return ky.digest
}

// Equals uses key digests to compare key equality.
func (ky *Key) Equals(other *Key) bool {
	return bytes.Equal(ky.digest, other.digest)
}

// String implements Stringer interface and returns string representation of key.
func (ky *Key) String() string {
	if ky == nil {
		return ""
	}

	if ky.userKey != nil {
		return fmt.Sprintf("%s:%s:%s:%v", ky.namespace, ky.setName, ky.userKey.String(), Buffer.BytesToHexString(ky.digest))
	}
	return fmt.Sprintf("%s:%s::%v", ky.namespace, ky.setName, Buffer.BytesToHexString(ky.digest))
}

// NewKey initializes a key from namespace, optional set name and user key.
// The set name and user defined key are converted to a digest before sending to the server.
// The server handles record identifiers by digest only.
func NewKey(namespace string, setName string, key interface{}) (newKey *Key, err error) {
	newKey = &Key{
		namespace: namespace,
		setName:   setName,
		userKey:   NewValue(key),
	}

	newKey.digest, err = computeDigest(newKey)

	return newKey, err
}

// NewKeyWithDigest initializes a key from namespace, optional set name and user key.
// The server handles record identifiers by digest only.
func NewKeyWithDigest(namespace string, setName string, key interface{}, digest []byte) (newKey *Key, err error) {
	newKey = &Key{
		namespace: namespace,
		setName:   setName,
		userKey:   NewValue(key),
	}

	if err = newKey.SetDigest(digest); err != nil {
		return nil, err
	}
	return newKey, err
}

// SetDigest sets a custom hash
func (ky *Key) SetDigest(digest []byte) error {
	if len(digest) != 20 {
		return NewAerospikeError(PARAMETER_ERROR, "Invalid digest: Digest is required to be exactly 20 bytes.")
	}
	ky.digest = digest
	return nil
}

// Generate unique server hash value from set name, key type and user defined key.
// The hash function is RIPEMD-160 (a 160 bit hash).
func computeDigest(key *Key) ([]byte, error) {
	keyType := key.userKey.GetType()

	if keyType == ParticleType.NULL {
		return nil, NewAerospikeError(PARAMETER_ERROR, "Invalid key: nil")
	}

	if keyType == ParticleType.MAP {
		return nil, NewAerospikeError(PARAMETER_ERROR, "Invalid key: Maps are not allowed. Iterartion on maps is random, and thus the digest is unstable.")
	}

	// retrieve hash from hash pool
	h := hashPool.Get().(hash.Hash)
	h.Reset()

	buf := keyBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteString(key.setName)
	buf.WriteByte(byte(keyType))
	buf.ReadFrom(key.userKey.reader())

	h.Write(buf.Bytes())
	res := h.Sum(nil)

	// put hash object back to the pool
	hashPool.Put(h)
	keyBufPool.Put(buf)

	return res, nil
}

// hash pool
var hashPool *Pool
var keyBufPool *Pool

func init() {
	hashPool = NewPool(512)
	hashPool.New = func(params ...interface{}) interface{} {
		return ripemd160.New()
	}

	keyBufPool = NewPool(512)
	keyBufPool.New = func(params ...interface{}) interface{} {
		return new(bytes.Buffer)
	}
}
