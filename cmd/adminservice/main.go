package main

import (
	"github.com/leveros/leveros/admin"
	leverapi "github.com/leveros/leveros/api"
	"github.com/leveros/leveros/cmd"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/store"
)

// PackageName is the name of this package.
const PackageName = cmd.PackageName + ".adminservice"

var logger = leverutil.GetLogger(PackageName, "main")

func main() {
	config.Initialize()
	leverutil.UpdateLoggingSettings()

	server, err := leverapi.NewServer()
	if err != nil {
		logger.WithFields("err", err).Fatal("Cannot create Lever server")
	}

	as, err := store.NewAerospike()
	if err != nil {
		logger.WithFields("err", err).Fatal("Cannot get aerospike client")
	}
	ad, err := admin.NewAdmin(as)
	if err != nil {
		logger.WithFields("err", err).Fatal("Cannot create Admin obj")
	}
	err = server.RegisterHandlerObject(ad)
	if err != nil {
		logger.WithFields("err", err).Fatal("Cannot register Lever handler")
	}

	err = server.Serve()
	if err != nil {
		logger.WithFields("err", err).Fatal("Error while serving")
	}
}
