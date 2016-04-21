package devlogger

import (
	"time"

	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/scale"
	syslog "gopkg.in/mcuadros/go-syslog.v2"
)

// PackageName is the name of this package.
const PackageName = "devlogger"

var (
	// DevLoggerListenPortFlag is the port to listen to for syslog messages.
	DevLoggerListenPortFlag = config.DeclareString(
		PackageName, "devLoggerListenPort", "6514")
	// InstanceIDFlag is the instance ID of the devlogger. Note: This is a
	// different instance ID than leverutil.InstanceIDFlag because they serve
	// different things.
	InstanceIDFlag = config.DeclareString(
		PackageName, "instanceID", leverutil.RandomID())
	// DisableFlag disables the devlogger server.
	DisableFlag = config.DeclareBool(PackageName, "disable")
)

var logger = leverutil.GetLogger(PackageName, "DevLogger")

// DevLoggerService is the name of the devlogger internal service.
const DevLoggerService = "devlogger"

// DevLogger is a syslog server meant to be used only for dev instances of
// Lever. It doesn't have the necessary redundancy and safeguards in place for
// production use.
type DevLogger struct {
	server     *syslog.Server
	serviceSKA *scale.SelfKeepAlive
	channel    syslog.LogPartsChannel
}

// NewDevLogger returns a new DevLogger.
func NewDevLogger(ownIP string) (*DevLogger, error) {
	if DisableFlag.Get() {
		return nil, nil
	}
	dl := &DevLogger{
		server:  syslog.NewServer(),
		channel: make(syslog.LogPartsChannel),
	}
	dl.server.SetFormat(syslog.RFC5424)
	dl.server.SetHandler(syslog.NewChannelHandler(dl.channel))
	dl.server.ListenTCP("0.0.0.0:" + DevLoggerListenPortFlag.Get())
	dl.server.Boot()
	go dl.worker()

	// Register service.
	instanceID := InstanceIDFlag.Get()
	serviceAddr := ownIP + ":" + DevLoggerListenPortFlag.Get()
	serviceTTL := 30 * time.Second
	err := scale.RegisterServiceLocal(
		DevLoggerService, instanceID, serviceAddr, serviceTTL)
	if err != nil {
		dl.server.Kill()
		close(dl.channel)
		return nil, err
	}
	dl.serviceSKA = scale.NewServiceSelfKeepAlive(instanceID, serviceTTL/2)

	return dl, nil
}

// Close closes the server.
func (dl *DevLogger) Close() {
	dl.serviceSKA.Stop()
	err := scale.DeregisterService(InstanceIDFlag.Get())
	if err != nil {
		logger.WithFields("err", err).Error(
			"Error deregistering devlogger service")
	}
	dl.server.Kill()
	close(dl.channel)
}

func (dl *DevLogger) worker() {
	for logParts := range dl.channel {
		logger.WithFields("parts", logParts).Debug("Log line")
	}
}
