// Package main uploads a config file to consul.
package main

import (
	"log"

	"github.com/leveros/leveros/config"
)

var (
	// FileFlag is the config file to upload to consul.
	FileFlag = config.DeclareString("", "file", "")
	// ServiceFlag is the service the config belongs to.
	ServiceFlag = config.DeclareString("", "service", "")
)

func main() {
	config.Initialize()

	err := config.UploadFile(FileFlag.Get(), ServiceFlag.Get())
	if err != nil {
		log.Fatalf("Unable to upload config: %v\n", err)
	}
}
