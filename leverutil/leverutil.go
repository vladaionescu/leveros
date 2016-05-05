// Package leverutil provides general utility functions that are not specific to
// Lever.
package leverutil

import (
	"math/rand"
	"sync"
	"time"

	"github.com/leveros/leveros/config"
)

// PackageName is the name of this package.
const PackageName = "leverutil"

var (
	// ServiceFlag is the service name that will be attached to log entries.
	ServiceFlag = config.DeclareString("", "service", "")
	// InstanceIDFlag is the instance ID that will be attached to log entries.
	InstanceIDFlag = config.DeclareString("", "instanceID", RandomID())

	// ContainerNameFlag is the name of the container this is running as (needs
	// to be set correctly from command line arg). This is used to determine
	// the containerID.
	ContainerNameFlag = config.DeclareString("", "containerName", "")
)

var (
	baseRand     = rand.New(rand.NewSource(time.Now().UnixNano()))
	baseRandLock sync.Mutex
)

var randPool = &sync.Pool{
	New: func() interface{} {
		baseRandLock.Lock()
		moreSeed := baseRand.Int31()
		baseRandLock.Unlock()
		seed := time.Now().UnixNano()
		return rand.New(rand.NewSource((seed << 32) | int64(moreSeed)))
	},
}

// GetRand returns a readily seeded rand from a pool.
func GetRand() (tmpRand *rand.Rand) {
	return randPool.Get().(*rand.Rand)
}

// PutRand returns the rand to the pool.
func PutRand(tmpRand *rand.Rand) {
	randPool.Put(tmpRand)
}

const idAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"0123456789"

// RandomID returns a random string identifier.
func RandomID() string {
	// Rand is not thread-safe. So get our own rand instance.
	tmpRand := GetRand()
	b := make([]byte, 32)
	for i := range b {
		b[i] = idAlphabet[tmpRand.Intn(len(idAlphabet))]
	}
	PutRand(tmpRand)
	return string(b)
}

const hostNameAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

// RandomHostName returns a random name that can be used as part of a host name.
func RandomHostName() string {
	// Rand is not thread-safe. So get our own rand instance.
	tmpRand := GetRand()
	b := make([]byte, 50)
	for i := range b {
		b[i] = hostNameAlphabet[tmpRand.Intn(len(hostNameAlphabet))]
	}
	PutRand(tmpRand)
	return string(b)
}
