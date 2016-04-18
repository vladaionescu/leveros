package store

import (
	"strconv"
	"strings"
	"time"

	aerospike "github.com/aerospike/aerospike-client-go"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/scale"
)

const leverOSNamespace = "leveros"

var (
	// AerospikeFromConsulFlag indicates whether Consul should be used to get
	// an Aerospike client.
	AerospikeFromConsulFlag = config.DeclareBool(
		PackageName, "aerospikeFromConsul")
	// AerospikeFixedAddrFlag indicates a fixed address which is used if
	// AerospikeFromConsulFlag is false.
	AerospikeFixedAddrFlag = config.DeclareString(
		PackageName, "aerospikeFixedAddr", "leverosaerospike:3000")
)

// NewAerospike returns a new Aerospike client.
func NewAerospike() (as *aerospike.Client, err error) {
	var target string
	if AerospikeFromConsulFlag.Get() {
		for retry := 0; retry < 15; retry++ {
			target, _, err = scale.DereferenceService("aerospike")
			if err == nil {
				break
			}
			if err == scale.ErrServiceNotFound ||
				strings.Contains(err.Error(), "network is unreachable") ||
				strings.Contains(err.Error(), "no such host") {
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, err
		}
		if err != nil {
			return nil, err
		}
	} else {
		target = AerospikeFixedAddrFlag.Get()
	}

	ipPort := strings.Split(target, ":")
	port, err := strconv.Atoi(ipPort[1])
	if err != nil {
		return nil, err
	}

	for retry := 0; retry < 15; retry++ {
		as, err = aerospike.NewClient(ipPort[0], port)
		if err == nil {
			return as, nil
		}
		if strings.Contains(err.Error(), "Failed to connect") {
			time.Sleep(1 * time.Second)
			continue
		}
		return nil, err
	}
	return nil, err
}
