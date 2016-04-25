package main

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/codegangsta/cli"
	leverapi "github.com/leveros/leveros/api"
	"github.com/leveros/leveros/cmd"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/leverutil"
)

// PackageName is the name of this package.
const PackageName = cmd.PackageName + ".adminservice"

// Version is the version of Lever OS. This variable is set at build time.
var Version string

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
	app.Usage = "Command line interface for Lever OS"
	// TODO: Need proper installation logic for bash completion to work.
	app.EnableBashCompletion = true
	app.Version = Version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "env, E",
			Value:  "",
			EnvVar: "LEVEROS_ENVIRONMENT",
			Usage: "The name of the environment the client interacts " +
				"with.",
			Destination: &flagEnv,
		},
		cli.StringFlag{
			Name:   "host, H",
			Value:  "",
			EnvVar: "LEVEROS_IP_PORT",
			Usage: "The address to direct the client to, if different from " +
				"the env name.",
			Destination: &flagHost,
		},
	}
	app.Commands = []cli.Command{
		{
			Name:      "deploy",
			Usage:     "Deploy a directory as a Lever service.",
			ArgsUsage: "<env> [<dir>]",
			Action:    actionDeploy,
		},
		{
			Name: "invoke",
			Usage: "Invoke a Lever method with provided args. If the " +
				"first <jsonarg> is --, then args are assumed to be of type " +
				"bytes and are read from standard input.",
			ArgsUsage: "<url> [jsonargs...]",
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
			ArgsUsage: "<url> [jsonargs...]",
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
	adminEnv := core.AdminEnvFlag.Get()
	if flagEnv != "" {
		adminEnv = flagEnv
	}
	if strings.HasSuffix(adminEnv, ".lever") {
		host = detectLeverOSIPPort()
	}
	if flagHost != "" {
		host = flagHost
	}

	destEnv := ctx.Args().First()
	if destEnv == "" {
		logger.Fatal("Destination environment not specified")
	}
	serviceDir := ctx.Args().Get(1)
	if serviceDir == "" {
		serviceDir = "."
	}
	err := leverapi.DeployServiceDir(
		adminEnv, host, destEnv, serviceDir)
	if err != nil {
		logger.WithFields("err", err).Fatal("Error trying to deploy service")
	}
}

func actionInvoke(ctx *cli.Context) {
	leverURL, err := core.ParseLeverURL(ctx.Args().Get(0))
	if err != nil {
		logger.WithFields("err", err).Fatal("Invalid lever URL")
	}
	if leverURL.Environment == "" {
		leverURL.Environment = flagEnv
	}
	if leverURL.Environment == "" {
		leverURL.Environment = core.DefaultDevEnvFlag.Get()
	}

	client, err := leverapi.NewClient()
	if err != nil {
		logger.WithFields("err", err).Fatal("Error creating client")
	}
	if strings.HasSuffix(leverURL.Environment, ".lever") {
		client.ForceHost = detectLeverOSIPPort()
	}
	if flagHost != "" {
		client.ForceHost = flagHost
	}

	if ctx.NArg() > 1 && ctx.Args().Get(1) == "--" {
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
	var reply interface{}
	err = client.InvokeURL(&reply, leverURL.String(), args...)
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
	leverURL, err := core.ParseLeverURL(ctx.Args().Get(0))
	if err != nil {
		logger.WithFields("err", err).Fatal("Invalid lever URL")
	}
	if leverURL.Environment == "" {
		leverURL.Environment = flagEnv
	}
	if leverURL.Environment == "" {
		leverURL.Environment = core.DefaultDevEnvFlag.Get()
	}

	client, err := leverapi.NewClient()
	if err != nil {
		logger.WithFields("err", err).Fatal("Error creating client")
	}
	if strings.HasSuffix(leverURL.Environment, ".lever") {
		client.ForceHost = detectLeverOSIPPort()
	}
	if flagHost != "" {
		client.ForceHost = flagHost
	}

	var args []interface{}
	for index := 1; index < ctx.NArg(); index++ {
		rawArg := ctx.Args().Get(index)
		var arg interface{}
		err = json.Unmarshal([]byte(rawArg), &arg)
		if err != nil {
			logger.WithFields("err", err, "jsonArg", rawArg).Fatal(
				"Error parsing JSON arg")
		}
		args = append(args, arg)
	}
	stream, err := client.InvokeChanURL(leverURL.String(), args...)
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
			var size int
			size, err = stdin.Read(chunk)
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
			var line string
			line, err = stdin.ReadString('\n')
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

func detectLeverOSIPPort() (ipPort string) {
	ipPort = os.Getenv("LEVEROS_IP_PORT")
	if ipPort != "" {
		return ipPort
	}
	ipPort = detectLeverOSIPPortOnDockerMachine()
	if ipPort != "" {
		return ipPort
	}
	ipPort = "127.0.0.1:8080"
	logger.WithFields("ipPort", ipPort).Warning(
		"Could not detect Lever OS ip+port. Using a hardcoded value.")
	return ipPort
}

func detectLeverOSIPPortOnDockerMachine() (ipPort string) {
	dockerMachineName := os.Getenv("DOCKER_MACHINE_NAME")
	if dockerMachineName == "" {
		return ""
	}
	ip, err := exec.Command("docker-machine", "ip", dockerMachineName).Output()
	if err != nil {
		logger.WithFields("err", err).Error(
			"Error running docker-machine command")
		return ""
	}
	ipStr := strings.TrimSpace(string(ip))
	if ipStr == "" {
		return ""
	}
	ipPort = ipStr + ":8080"
	logger.WithFields("ipPort", ipPort).Info(
		"Using Lever OS IP+port inferred from docker-machine IP")
	return ipPort
}
