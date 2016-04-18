package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	consulapi "github.com/hashicorp/consul/api"
)

// PackageName is the name of this package.
const PackageName = "config"

const consulPrefix = "conf/"

// UploadFile uploads provided config file to consul for use as flags.
func UploadFile(filePath string, service string) error {
	byteData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	// Check contents are valid json and re-encode for minimal formatting.
	var data map[string]interface{}
	err = json.Unmarshal(byteData, &data)
	if err != nil {
		return err
	}
	byteData, err = json.Marshal(&data)
	if err != nil {
		return err
	}

	client := GetConsulClient()
	kv := client.KV()
	kv.Put(&consulapi.KVPair{
		Key:   consulPrefix + service,
		Value: byteData,
	}, nil)

	return nil
}

const (
	typeString = iota
	typeInt
	typeFloat
	typeBool
	typeDuration
)

type flagEntry struct {
	commandLineValue interface{}
	consulValue      interface{}
	defaultValue     interface{}
	flagType         int
}

var (
	// Guard the flags map and the entries within.
	flagsRWMutex sync.RWMutex
	flags        = make(map[string]*flagEntry)

	// RestArgs represents the list of command line args remaining after all
	// the flags.
	RestArgs []string

	consulConfigChecksum uint32
	consulConfigSize     int
)

func declare(key string, defaultValue interface{}, flagType int) *flagEntry {
	flagsRWMutex.Lock()
	defer flagsRWMutex.Unlock()
	_, alreadyExists := flags[key]
	if alreadyExists {
		log.Fatalf("Flag re-declaration of %s\n", key)
	}
	entry := &flagEntry{
		defaultValue: defaultValue,
		flagType:     flagType,
	}
	flags[key] = entry
	return entry
}

func (entry *flagEntry) get() interface{} {
	flagsRWMutex.RLock()
	defer flagsRWMutex.RUnlock()
	return entry.getInternal()
}

func (entry *flagEntry) getInternal() interface{} {
	if entry.commandLineValue != nil {
		return entry.commandLineValue
	}
	if entry.consulValue != nil {
		return entry.consulValue
	}
	return entry.defaultValue
}

// FlagString represents a string configuration.
type FlagString struct {
	entry *flagEntry
}

// DeclareString can be used to declare a new string flag with a default value.
func DeclareString(pack, name, defaultValue string) *FlagString {
	key := getKey(pack, name)
	return &FlagString{entry: declare(key, defaultValue, typeString)}
}

// Get retrieves the value of flag.
func (flag *FlagString) Get() string {
	return flag.entry.get().(string)
}

// FlagInt represents an integer configuration.
type FlagInt struct {
	entry *flagEntry
}

// DeclareInt can be used to declare a new int flag with a default value.
func DeclareInt(pack, name string, defaultValue int) *FlagInt {
	key := getKey(pack, name)
	return &FlagInt{entry: declare(key, defaultValue, typeInt)}
}

// Get retrieves the value of flag.
func (flag *FlagInt) Get() int {
	return flag.entry.get().(int)
}

// FlagFloat represents a float configuration.
type FlagFloat struct {
	entry *flagEntry
}

// DeclareFloat can be used to declare a new float flag with a default value.
func DeclareFloat(pack, name string, defaultValue float64) *FlagFloat {
	key := getKey(pack, name)
	return &FlagFloat{entry: declare(key, defaultValue, typeFloat)}
}

// Get retrieves the value of flag.
func (flag *FlagFloat) Get() float64 {
	return flag.entry.get().(float64)
}

// FlagBool represents a boolean configuration.
type FlagBool struct {
	entry *flagEntry
}

// DeclareBool can be used to declare a new bool flag with a default value
// false.
func DeclareBool(pack, name string) *FlagBool {
	key := getKey(pack, name)
	return &FlagBool{entry: declare(key, false, typeBool)}
}

// Get retrieves the value of flag.
func (flag *FlagBool) Get() bool {
	return flag.entry.get().(bool)
}

// FlagDuration represents a duration configuration.
type FlagDuration struct {
	entry *flagEntry
}

// DeclareDuration can be used to declare a new duration flag with a default
// value.
func DeclareDuration(
	pack, name string, defaultValue time.Duration) *FlagDuration {
	key := getKey(pack, name)
	return &FlagDuration{entry: declare(key, defaultValue, typeDuration)}
}

// Get retrieves the value of flag.
func (flag *FlagDuration) Get() time.Duration {
	return flag.entry.get().(time.Duration)
}

// ConfigFlag is the service identifier used to get/monitor the flags from
// consul.
var ConfigFlag = DeclareString("", "config", "")

// Initialize needs to be called before any call to Get. It parses command line
// flags and begins monitoring of consul key holding the flags for this service.
func Initialize() {
	flagsRWMutex.Lock()
	parseArgs()
	parseEnv()
	flagsRWMutex.Unlock()
	monitorConsul()
}

func parseArgs() {
	var entry *flagEntry
	var expectValue bool
	var isRestArgs bool
	for _, arg := range os.Args[1:] {
		if expectValue {
			var err error
			entry.commandLineValue, err = parseFromString(arg, entry.flagType)
			if err != nil {
				log.Fatalf("Unable to parse flag %s\n", arg)
			}
			expectValue = false
			continue
		}
		if isRestArgs {
			RestArgs = append(RestArgs, arg)
			continue
		}
		if len(arg) < 2 || arg[:2] != "--" {
			RestArgs = append(RestArgs, arg)
			isRestArgs = true
			continue
		}
		if arg == "--" {
			isRestArgs = true
			continue
		}

		flag := arg[2:]
		var exists bool
		entry, exists = flags[flag]
		if !exists {
			log.Fatalf("Invalid flag %s\n", arg)
		}
		expectValue = true
	}
}

func parseEnv() {
	for key, entry := range flags {
		if entry.commandLineValue != nil {
			continue
		}
		envKey := "LEVEROS_" + strings.Replace(key, ".", "_", -1)
		envValue := os.Getenv(envKey)
		if envValue != "" {
			var err error
			entry.commandLineValue, err = parseFromString(
				envValue, entry.flagType)
			if err != nil {
				log.Fatalf(
					"Unable to parse env var %s as %s\n", envKey, envValue)
			}
		}
	}
}

func monitorConsul() {
	service := ConfigFlag.Get()
	if service == "" {
		return
	}

	client := GetConsulClient()
	kv := client.KV()

	var (
		pair *consulapi.KVPair
		qm   *consulapi.QueryMeta
		err  error
	)
	for retry := 0; retry < 20; retry++ {
		pair, qm, err = kv.Get(
			consulPrefix+service, &consulapi.QueryOptions{
				RequireConsistent: true,
			})
		if err != nil {
			if consulapi.IsServerError(err) {
				// Consul unavailable. Try again.
				time.Sleep(1 * time.Second)
				continue
			} else {
				break
			}
		}
		if pair == nil {
			// Config does not exist. Maybe not yet uploaded. Try again.
			err = fmt.Errorf("Config not found")
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	if err != nil {
		log.Fatalf(
			"Error trying to get config for the first time from consul: %v\n",
			err)
	}

	updateConsulFlags(pair.Value)

	go monitorConsulGoroutine(kv, service, qm.LastIndex)
}

func monitorConsulGoroutine(kv *consulapi.KV, service string, lastIndex uint64) {
	for {
		pair, qm, err := kv.Get(
			consulPrefix+service,
			&consulapi.QueryOptions{
				WaitIndex:         lastIndex,
				RequireConsistent: true,
			})
		if err != nil {
			if consulapi.IsServerError(err) {
				// Consul unavailable. Try again.
				time.Sleep(1 * time.Second)
				continue
			}
			if strings.Contains(err.Error(), "read: connection timed out") {
				// Try again.
				time.Sleep(1 * time.Second)
				continue
			}

			log.Fatalf("Error monitoring config in Consul: %v\n", err)
		}
		if pair == nil {
			log.Fatalf("Config in consul has been deleted\n")
		}

		updateConsulFlags(pair.Value)
		lastIndex = qm.LastIndex
	}
}

func updateConsulFlags(byteData []byte) {
	checksum := crc32.ChecksumIEEE(byteData)
	if len(byteData) == consulConfigSize && checksum == consulConfigChecksum {
		// Assume config did not change. Don't do anything.
		return
	}
	consulConfigSize = len(byteData)
	consulConfigChecksum = checksum

	var data map[string]interface{}
	err := json.Unmarshal(byteData, &data)
	if err != nil {
		log.Fatalf("Consul config is not valid JSON\n")
	}

	flagsRWMutex.Lock()
	defer flagsRWMutex.Unlock()

	// Clear existing flags.
	for _, entry := range flags {
		entry.consulValue = nil
	}

	// Update with data from consul.
	for key, value := range data {
		entry, exists := flags[key]
		if !exists {
			log.Fatalf("Invalid flag %s in consul config\n", key)
		}

		strValue, isStr := value.(string)
		if !isStr {
			var strFormatted []byte
			strFormatted, err = json.Marshal(value)
			if err != nil {
				log.Fatalf("Unable to marshal value back to json\n")
			}
			strValue = string(strFormatted)
		}

		entry.consulValue, err = parseFromString(strValue, entry.flagType)
		if err != nil {
			log.Fatalf(
				"Unable to parse flag %s from consul: %s\n", key, strValue)
		}
	}

	var buffer bytes.Buffer
	buffer.WriteString("Config updated from consul. Current configuration:\n")
	for key, entry := range flags {
		buffer.WriteString(fmt.Sprintf("\t%s=%v\n", key, entry.getInternal()))
	}
	// TODO: It might be necessary to put this logging through the usual
	//       leverutil.logging stuff. Problem is that there's a circular
	//       dependency issue. Possible solution: Move logging under config.
	log.Printf(buffer.String())
}

func getKey(pack, name string) string {
	if pack == "" {
		return name
	}
	return pack + "." + name
}

func parseFromString(
	value string, flagType int) (parsed interface{}, err error) {
	switch flagType {
	case typeString:
		parsed = value
		err = nil
	case typeInt:
		parsed, err = strconv.Atoi(value)
	case typeFloat:
		parsed, err = strconv.ParseFloat(value, 64)
	case typeBool:
		parsed, err = strconv.ParseBool(value)
	case typeDuration:
		parsed, err = time.ParseDuration(value)
	}
	return
}
