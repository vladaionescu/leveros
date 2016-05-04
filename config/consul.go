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
	initOnce     sync.Once
	consulClient *consulapi.Client
)

// GetConsulClient returns a readily-configured consul client.
func GetConsulClient() *consulapi.Client {
	initOnce.Do(func() {
		apiConfig := consulapi.DefaultConfig()
		apiConfig.Address = ConsulAddressFlag.Get()
		var err error
		consulClient, err = consulapi.NewClient(apiConfig)
		if err != nil {
			panic(err)
		}
	})
	return consulClient
}
