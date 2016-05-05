// Package main initializes Lever OS's database.
package main

import (
	"github.com/leveros/leveros/cmd"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/store"
)

// PackageName is the name of this package.
const PackageName = cmd.PackageName + ".inittables"

var logger = leverutil.GetLogger(PackageName, "main")

func main() {
	config.Initialize()
	leverutil.UpdateLoggingSettings()

	as, err := store.NewAerospike()
	if err != nil {
		logger.WithFields("err", err).Fatal("Failed to get aerospike client")
	}

	err = store.InitServiceTable(as)
	if err != nil {
		logger.WithFields("err", err).Fatal("Failed to init service table")
	}

	err = store.NewEnv(as, core.AdminEnvFlag.Get(), "The admin environment")
	if err != nil {
		logger.WithFields("err", err).Warning("Failed to create admin env")
	}

	err = store.NewEnv(as, core.DefaultDevEnvFlag.Get(), "The default environment")
	if err != nil {
		logger.WithFields("err", err).Warning("Failed to create new env")
	}

	err = store.NewService(
		as, core.AdminEnvFlag.Get(), "admin", "The admin environment", true)
	if err != nil {
		logger.WithFields("err", err).Warning("Failed to create admin service")
	}
	err = store.SetServiceLiveCodeVersion(as, core.AdminEnvFlag.Get(), "admin", 1)
	if err != nil {
		logger.WithFields("err", err).Warning(
			"Failed to set admin live code version")
	}
}
