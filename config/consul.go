package config

import (
	"sync"

	consulapi "github.com/hashicorp/consul/api"
)

var (
	// ConsulAddressFlag is the address consul's HTTP interface listens on in
	// format <ip>:<port>.
	ConsulAddressFlag = DeclareString(
		PackageName, "consulAddress", "leverosconsul:8500")
)

var (
	consulClient     *consulapi.Client
	consulClientLock sync.RWMutex
)

// GetConsulClient returns a readily-configured consul client.
func GetConsulClient() *consulapi.Client {
	consulClientLock.RLock()
	if consulClient == nil {
		consulClientLock.RUnlock()
		consulClientLock.Lock()
		if consulClient != nil {
			consulClientLock.Unlock()
			return GetConsulClient()
		}

		apiConfig := consulapi.DefaultConfig()
		apiConfig.Address = ConsulAddressFlag.Get()
		var err error
		consulClient, err = consulapi.NewClient(apiConfig)
		if err != nil {
			panic(err)
		}
		defer consulClientLock.Unlock()
		return consulClient
	}
	defer consulClientLock.RUnlock()
	return consulClient
}
