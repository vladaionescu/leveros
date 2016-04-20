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
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	. "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
)

// Validates a Database server node
type nodeValidator struct {
	name       string
	aliases    []*Host
	address    string
	useNewInfo bool //= true
	cluster    *Cluster

	supportsFloat, supportsBatchIndex, supportsReplicasAll, supportsGeo bool
}

// Generates a node validator
func newNodeValidator(cluster *Cluster, host *Host, timeout time.Duration) (*nodeValidator, error) {
	newNodeValidator := &nodeValidator{
		useNewInfo: true,
		cluster:    cluster,
	}

	if err := newNodeValidator.setAliases(host); err != nil {
		return nil, err
	}

	if err := newNodeValidator.setAddress(timeout); err != nil {
		return nil, err
	}

	return newNodeValidator, nil
}

func (ndv *nodeValidator) setAliases(host *Host) error {
	// IP addresses do not need a lookup
	ip := net.ParseIP(host.Name)
	if ip != nil {
		aliases := make([]*Host, 1)
		aliases[0] = NewHost(host.Name, host.Port)
		ndv.aliases = aliases
	} else {
		addresses, err := net.LookupHost(host.Name)
		if err != nil {
			Logger.Error("HostLookup failed with error: ", err)
			return err
		}
		aliases := make([]*Host, len(addresses))
		for idx, addr := range addresses {
			aliases[idx] = NewHost(addr, host.Port)
		}
		ndv.aliases = aliases
	}
	Logger.Debug("Node Validator has %d nodes.", len(ndv.aliases))
	return nil
}

func (ndv *nodeValidator) setAddress(timeout time.Duration) error {
	for _, alias := range ndv.aliases {
		address := net.JoinHostPort(alias.Name, strconv.Itoa(alias.Port))
		conn, err := NewConnection(address, time.Second)
		if err != nil {
			return err
		}

		defer conn.Close()

		// need to authenticate
		if err := conn.Authenticate(ndv.cluster.user, ndv.cluster.Password()); err != nil {
			// Socket not authenticated. Do not put back into pool.
			conn.Close()

			return err
		}

		if err := conn.SetTimeout(timeout); err != nil {
			return err
		}

		infoMap, err := RequestInfo(conn, "node", "build", "features")
		if err != nil {
			return err
		}
		if nodeName, exists := infoMap["node"]; exists {
			ndv.name = nodeName
			ndv.address = address

			// set features
			if features, exists := infoMap["features"]; exists {
				ndv.setFeatures(features)
			}

			// Check new info protocol support for >= 2.6.6 build
			if buildVersion, exists := infoMap["build"]; exists {
				v1, v2, v3, err := parseVersionString(buildVersion)
				if err != nil {
					Logger.Error(err.Error())
					return err
				}
				ndv.useNewInfo = v1 > 2 || (v1 == 2 && (v2 > 6 || (v2 == 6 && v3 >= 6)))
			}
		}
	}
	return nil
}

func (ndv *nodeValidator) setFeatures(features string) {
	featureList := strings.Split(features, ";")
	for i := range featureList {
		switch featureList[i] {
		case "float":
			ndv.supportsFloat = true
		case "batch-index":
			ndv.supportsBatchIndex = true
		case "replicas-all":
			ndv.supportsReplicasAll = true
		case "geo":
			ndv.supportsGeo = true
		}
	}
}

// parses a version string
var r = regexp.MustCompile(`(\d+)\.(\d+)\.(\d+).*`)

func parseVersionString(version string) (int, int, int, error) {
	vNumber := r.FindStringSubmatch(version)
	if len(vNumber) < 4 {
		return -1, -1, -1, NewAerospikeError(PARSE_ERROR, "Invalid build version string in Info: "+version)
	}
	v1, err1 := strconv.Atoi(vNumber[1])
	v2, err2 := strconv.Atoi(vNumber[2])
	v3, err3 := strconv.Atoi(vNumber[3])
	if err1 == nil && err2 == nil && err3 == nil {
		return v1, v2, v3, nil
	}
	Logger.Error("Invalid build version string in Info: " + version)
	return -1, -1, -1, NewAerospikeError(PARSE_ERROR, "Invalid build version string in Info: "+version)
}
