package main

import (
	"bufio"
	"encoding/json"
	"io"
	"os"

	"github.com/codegangsta/cli"
	leverapi "github.com/leveros/leveros/api"
	"github.com/leveros/leveros/cmd"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/leverutil"
)

// PackageName is the name of this package.
const PackageName = cmd.PackageName + ".adminservice"

var logger = leverutil.GetLogger(PackageName, "main")

var (
	flagEnv  string
	flagHost string

	flagPrettyPrint bool
	flagBytes       bool
)

func main() {
	leverutil.UpdateLoggingSettings()

	app := cli.NewApp()
	app.Name = "lever"
	app.Usage = "Client for LeverOS"
	// TODO: Need proper installation logic for bash completion to work.
	app.EnableBashCompletion = true
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "env, E",
			Value:  core.DefaultDevEnvFlag.Get(),
			EnvVar: "LEVEROS_ENVIRONMENT",
			Usage: "The name of the environment the client interacts " +
				"with.",
			Destination: &flagEnv,
		},
		cli.StringFlag{
			Name:   "host, H",
			Value:  "",
			EnvVar: "LEVEROS_HOST_ADDR",
			Usage: "The address to direct the client to, if different from " +
				"the env name. Defaults to $LEVEROS_LISTEN_IP_PORT if env " +
				"is " + core.DefaultDevEnvFlag.Get() + ".",
			Destination: &flagHost,
		},
	}
	app.Commands = []cli.Command{
		{
			Name:      "deploy",
			Usage:     "Deploy a directory as a Lever service.",
			ArgsUsage: "<dir>",
			Action:    actionDeploy,
		},
		{
			Name: "invoke",
			Usage: "Invoke a Lever method with provided args. If the " +
				"first <jsonarg> is --, then args are assumed to be of type " +
				"bytes and are read from standard input.",
			ArgsUsage: "<url> <method> [jsonargs...]",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name: "pretty",
					Usage: "Pretty print the JSON reply. " +
						"No effect if the reply is of type bytes.",
					Destination: &flagPrettyPrint,
				},
			},
			Action: actionInvoke,
		},
		{
			Name: "stream",
			Usage: "Invoke a Lever streaming method with provided args. " +
				"The stream communication takes place via standard IO. " +
				"While it is possible for the stream itself to be of type " +
				"bytes, this command does not allow for the args of the " +
				"invokation to be of type bytes.",
			ArgsUsage: "<url> <method> [jsonargs...]",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name: "bytes",
					Usage: "Stdin will be interpreted as byte " +
						"chunks rather than JSON.",
					Destination: &flagBytes,
				},
				cli.BoolFlag{
					Name: "pretty",
					Usage: "Pretty print the JSON reply. " +
						"No effect if the reply is of type bytes.",
					Destination: &flagPrettyPrint,
				},
			},
			Action: actionStream,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		logger.WithFields("err", err).Fatal("")
	}
}

func actionDeploy(ctx *cli.Context) {
	host := ""
	if flagHost != "" {
		host = flagHost
	}
	if flagEnv == core.DefaultDevEnvFlag.Get() {
		host = core.DefaultDevEnvAliasFlag.Get()
	}
	serviceDir := ctx.Args().First()
	err := leverapi.DeployServiceDir(
		core.AdminEnvFlag.Get(), host, flagEnv, serviceDir)
	if err != nil {
		logger.WithFields("err", err).Fatal("Error trying to deploy service")
	}
}

func actionInvoke(ctx *cli.Context) {
	peer, err := leverapi.ParseLeverURL(ctx.Args().Get(0))
	if err != nil {
		logger.WithFields("err", err).Fatal("Invalid lever URL")
	}
	if peer.Environment == "" {
		peer.Environment = flagEnv
	}

	client, err := leverapi.NewClient()
	if err != nil {
		logger.WithFields("err", err).Fatal("Error creating client")
	}
	if flagHost != "" {
		client.ForceHost = flagHost
	}
	if peer.Environment == core.DefaultDevEnvFlag.Get() {
		client.ForceHost = core.DefaultDevEnvAliasFlag.Get()
	}

	if ctx.NArg() > 2 && ctx.Args().Get(2) == "--" {
		// Byte args case.
		// TODO: Read bytes arg from stdin.
		logger.Fatal("Bytes from stdin not yet implemented")
		return
	}

	// JSON args case.
	var args []interface{}
	for index := 2; index < ctx.NArg(); index++ {
		rawArg := ctx.Args().Get(index)
		var arg interface{}
		err = json.Unmarshal([]byte(rawArg), &arg)
		if err != nil {
			logger.WithFields("err", err, "jsonArg", rawArg).Fatal(
				"Error parsing JSON arg")
		}
		args = append(args, arg)
	}
	method := ctx.Args().Get(1)
	endpoint, err := client.EndpointFromURL(peer.String())
	if err != nil {
		logger.WithFields("err", err).Fatal("Endpoint construction error")
	}
	var reply interface{}
	err = endpoint.Invoke(&reply, method, args...)
	if err != nil {
		logger.WithFields("err", err).Fatal("Invokation error")
	}
	bytes, isByteReply := reply.([]byte)
	if !isByteReply {
		if flagPrettyPrint {
			bytes, err = json.MarshalIndent(reply, "", "  ")
		} else {
			bytes, err = json.Marshal(reply)
		}
		if err != nil {
			logger.WithFields("err", err).Fatal(
				"Error turning reply into JSON form")
		}
	}

	stdout := bufio.NewWriter(os.Stdout)
	stdout.Write(bytes)
	if !isByteReply && flagPrettyPrint {
		stdout.WriteString("\n")
	}
	stdout.Flush()
}

func actionStream(ctx *cli.Context) {
	peer, err := leverapi.ParseLeverURL(ctx.Args().Get(0))
	if err != nil {
		logger.WithFields("err", err).Fatal("Invalid lever URL")
	}
	if peer.Environment == "" {
		peer.Environment = flagEnv
	}

	client, err := leverapi.NewClient()
	if err != nil {
		logger.WithFields("err", err).Fatal("Error creating client")
	}
	if flagHost != "" {
		client.ForceHost = flagHost
	}
	if peer.Environment == core.DefaultDevEnvFlag.Get() {
		client.ForceHost = core.DefaultDevEnvAliasFlag.Get()
	}

	var args []interface{}
	for index := 2; index < ctx.NArg(); index++ {
		rawArg := ctx.Args().Get(index)
		var arg interface{}
		err = json.Unmarshal([]byte(rawArg), &arg)
		if err != nil {
			logger.WithFields("err", err, "jsonArg", rawArg).Fatal(
				"Error parsing JSON arg")
		}
		args = append(args, arg)
	}
	method := ctx.Args().Get(1)
	endpoint, err := client.EndpointFromURL(peer.String())
	if err != nil {
		logger.WithFields("err", err).Fatal("Endpoint construction error")
	}
	stream, err := endpoint.InvokeChan(method, args...)
	if err != nil {
		logger.WithFields("err", err).Fatal("Invokation error")
	}

	receiveDone := make(chan struct{})
	go func() {
		stdout := bufio.NewWriter(os.Stdout)

		for {
			var msg interface{}
			err = stream.Receive(&msg)
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.WithFields("err", err).Fatal(
					"Error receiving from server")
			}

			msgBytes, isBytes := msg.([]byte)
			if isBytes {
				_, err = stdout.Write(msgBytes)
				if err != nil {
					logger.WithFields("err", err).Fatal(
						"Error writing to stdout")
				}
			} else {
				var jsonMsg []byte
				if flagPrettyPrint {
					jsonMsg, err = json.MarshalIndent(msg, "", "  ")
				} else {
					jsonMsg, err = json.Marshal(msg)
				}
				if err != nil {
					logger.WithFields("err", err).Fatal(
						"Error turning receiving msg into JSON")
				}
				stdout.Write(jsonMsg)
				stdout.WriteString("\n")
			}
		}

		err = stdout.Flush()
		if err != nil {
			logger.WithFields("err", err).Fatal("Error flushing stdout")
		}
		close(receiveDone)
	}()

	stdin := bufio.NewReader(os.Stdin)
	if flagBytes {
		chunk := make([]byte, 32*1024)
		for {
			size, err := stdin.Read(chunk)
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.WithFields("err", err).Fatal("Error reading from stdin")
			}

			err = stream.Send(chunk[:size])
			if err != nil {
				logger.WithFields("err", err).Fatal("Error sending chunk")
			}
		}
	} else {
		for {
			line, err := stdin.ReadString('\n')
			if err == io.EOF && line == "" {
				break
			}
			if err != nil && err != io.EOF {
				logger.WithFields("err", err).Fatal("Error reading from stdin")
			}

			var jsonMsg interface{}
			err = json.Unmarshal([]byte(line), &jsonMsg)
			if err != nil {
				logger.WithFields("err", err).Fatal(
					"Error parsing JSON from stdin")
			}

			err = stream.Send(jsonMsg)
			if err != nil {
				logger.WithFields("err", err).Fatal("Error sending JSON")
			}
		}
	}

	err = stream.Close()
	if err != nil {
		logger.WithFields("err", err).Fatal("Error trying to close stream")
	}
	<-receiveDone
}
