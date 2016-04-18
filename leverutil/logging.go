package leverutil

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/leveros/leveros/config"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	// LoggingFormatFlag is the format of log lines.
	LoggingFormatFlag = config.DeclareString(
		PackageName, "loggingFormat", "colortext")
	// LoggingLevelFlag is the minimum level to log.
	LoggingLevelFlag = config.DeclareString(
		PackageName, "loggingLevel", "debug")
	// LogInstanceAndServiceFlag causes all loggers to also log the instance ID
	// and service name of the process. Useful if logs from multiple sources
	// are merged and need to be filtered afterwards.
	LogInstanceAndServiceFlag = config.DeclareBool(
		PackageName, "logInstanceAndService")
)

// UpdateLoggingSettings initializes logging based on config params.
func UpdateLoggingSettings() {
	format := LoggingFormatFlag.Get()
	switch format {
	case "colortext":
		logrus.SetFormatter(&prefixed.TextFormatter{
			ShortTimestamp: true,
			ForceColors:    true,
		})
	case "text":
		logrus.SetFormatter(&logrus.TextFormatter{})
	case "json":
		logrus.SetFormatter(&logrus.JSONFormatter{})
	default:
		panic(fmt.Errorf("Invalid logging format %v", format))
	}

	level := LoggingLevelFlag.Get()
	switch level {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warning":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
	default:
		panic(fmt.Errorf("Invalid logging level %v", level))
	}
}

type field struct {
	key   string
	value interface{}
}

// Logger represents a logger with some metadata associated.
type Logger struct {
	fields []*field
}

// GetLogger returns a logger with provided package and name.
func GetLogger(pack, name string) *Logger {
	return new(Logger).WithFields(
		"prefix", pack+"."+name,
	)
}

// WithFields returns a new logger with provided metadata fields attached.
func (logger *Logger) WithFields(args ...interface{}) (ret *Logger) {
	if len(args)%2 != 0 {
		panic(fmt.Errorf("Invalid number of args"))
	}
	ret = &Logger{
		fields: make([]*field, len(logger.fields)+len(args)/2),
	}
	copy(ret.fields, logger.fields)

	j := 0
	for i := len(logger.fields); i < len(logger.fields)+len(args)/2; i++ {
		ret.fields[i] = &field{
			key:   args[j].(string),
			value: args[j+1],
		}
		j += 2
	}
	return ret
}

// Entry returns the logrus.Entry which represents the logger.
func (logger *Logger) Entry() *logrus.Entry {
	data := logrus.Fields{}
	for _, field := range logger.fields {
		data[field.key] = field.value
	}
	if LogInstanceAndServiceFlag.Get() {
		service := ServiceFlag.Get()
		if service != "" {
			data["__service"] = service
		}
		instanceID := InstanceIDFlag.Get()
		if instanceID != "" {
			data["__instanceID"] = instanceID
		}
	}
	return logrus.WithFields(data)
}

// Debug logs debug message.
func (logger *Logger) Debug(msg string) {
	logger.Entry().Debug(msg)
}

// Info logs info message.
func (logger *Logger) Info(msg string) {
	logger.Entry().Info(msg)
}

// Warning logs warning message.
func (logger *Logger) Warning(msg string) {
	logger.Entry().Warning(msg)
}

// Error logs error message.
func (logger *Logger) Error(msg string) {
	logger.Entry().Error(msg)
}

// Fatal logs fatal message and calls os.Exit(1).
func (logger *Logger) Fatal(msg string) {
	logger.Entry().Fatal(msg)
}

// Panic logs panic message and calls panic.
func (logger *Logger) Panic(msg string) {
	logger.Entry().Panic(msg)
}
