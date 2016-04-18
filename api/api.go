package api

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/leveros/leveros/core"
)

// PackageName is the name of this package.
const PackageName = "api"

var (
	// OwnEnvironment is the name of the environment the current service
	// operates in. This is meant to be "" if used from outside a lever service.
	OwnEnvironment = os.Getenv("LEVEROS_ENVIRONMENT")
	// OwnService is the name of the current service. This is meant to be "" if
	// used from outside a lever service.
	OwnService = os.Getenv("LEVEROS_SERVICE")
	// OwnInstanceID is the name of the current instance. This is meant to be
	// "" if used from outside a lever service.
	OwnInstanceID = os.Getenv("LEVEROS_INSTANCE_ID")
)

// BytesError is an error that can be represented as bytes.
type BytesError interface {
	GetBytes() []byte
	error
}

// IsChanMethod returns true if the name represents a valid streaming method
// name.
func IsChanMethod(name string) bool {
	return strings.HasSuffix(name, "Chan") ||
		strings.HasSuffix(name, "_chan") ||
		name == "chan"
}

//
// Utilities for encoding/decoding to/from our Protobuf-based JSON.

func encodeArgs(args []interface{}) (argsRet *core.JSONArray, err error) {
	argsRet = &core.JSONArray{}
	argsRet.Element = make([]*core.JSON, len(args))
	for index, value := range args {
		argsRet.Element[index], err = encodeArg(value)
		if err != nil {
			return nil, err
		}
	}
	return argsRet, nil
}

func encodeArg(arg interface{}) (argRet *core.JSON, err error) {
	argRet = &core.JSON{}
	if arg == nil {
		return argRet, nil
	}

	var num float64
	switch arg := arg.(type) {
	case string:
		argRet.JsonValueOneof = &core.JSON_JsonString{
			JsonString: arg,
		}
		return argRet, nil
	case bool:
		argRet.JsonValueOneof = &core.JSON_JsonBool{
			JsonBool: arg,
		}
		return argRet, nil
	case []interface{}:
		array := make([]*core.JSON, len(arg))
		for index, argEl := range arg {
			array[index], err = encodeArg(argEl)
			if err != nil {
				return nil, err
			}
		}
		argRet.JsonValueOneof = &core.JSON_JsonArray{
			JsonArray: &core.JSONArray{
				Element: array,
			},
		}
		return argRet, nil
	case *map[string]interface{}:
		obj, err := encodeArgMap(arg)
		if err != nil {
			return nil, err
		}
		argRet.JsonValueOneof = &core.JSON_JsonObject{
			JsonObject: obj,
		}
		return argRet, nil
	case map[string]interface{}:
		obj, err := encodeArgMap(&arg)
		if err != nil {
			return nil, err
		}
		argRet.JsonValueOneof = &core.JSON_JsonObject{
			JsonObject: obj,
		}
		return argRet, nil
	case int:
		num = float64(arg)
	case int8:
		num = float64(arg)
	case int16:
		num = float64(arg)
	case int32:
		num = float64(arg)
	case int64:
		num = float64(arg)
	case uint:
		num = float64(arg)
	case uint8:
		num = float64(arg)
	case uint16:
		num = float64(arg)
	case uint32:
		num = float64(arg)
	case uint64:
		num = float64(arg)
	case float32:
		num = float64(arg)
	case float64:
		num = arg
	default:
		// Struct or pointer to struct.
		// TODO: This is a hack. We encode into actual JSON and then let the
		//       json library walk the struct for us. This hurts performance
		//       a lot and should be improved with proper reflection.
		jsonEnc, err := json.Marshal(arg)
		if err != nil {
			return nil, err
		}
		data := make(map[string]interface{})
		err = json.Unmarshal(jsonEnc, &data)
		if err != nil {
			return nil, err
		}
		var properties []*core.JSONProperty
		for propName, value := range data {
			prop, err := encodeArg(value)
			if err != nil {
				return nil, err
			}
			properties = append(properties, &core.JSONProperty{
				Name:  propName,
				Value: prop,
			})
		}
		argRet.JsonValueOneof = &core.JSON_JsonObject{
			JsonObject: &core.JSONObject{
				Property: properties,
			},
		}
		return argRet, nil
	}
	// Number case.
	argRet.JsonValueOneof = &core.JSON_JsonNumber{
		JsonNumber: num,
	}
	return argRet, nil
}

func encodeArgMap(arg *map[string]interface{}) (*core.JSONObject, error) {
	var properties []*core.JSONProperty
	for propName, value := range *arg {
		prop, err := encodeArg(value)
		if err != nil {
			return nil, err
		}
		properties = append(properties, &core.JSONProperty{
			Name:  propName,
			Value: prop,
		})
	}
	return &core.JSONObject{
		Property: properties,
	}, nil
}

func decodeArgsAsValue(
	args *core.JSONArray, entry *handlerEntry, isStreaming bool) (
	valueArgs []reflect.Value, err error) {
	startIndex := 0
	if entry.isMethod {
		startIndex++
	}
	if entry.hasContext {
		startIndex++
	}
	if isStreaming {
		startIndex++
	}
	handlerType := reflect.TypeOf(entry.handler)
	endIndex := handlerType.NumIn()
	if handlerType.IsVariadic() {
		// Variadic arg will be dealt with separately.
		endIndex--
	}
	minArgs := endIndex - startIndex
	if handlerType.IsVariadic() {
		if len(args.GetElement()) < minArgs {
			return nil, fmt.Errorf(
				"Number of args does not match. Received %v but handling %v+",
				len(args.GetElement()), minArgs)
		}
	} else {
		if len(args.GetElement()) != minArgs {
			return nil, fmt.Errorf(
				"Number of args does not match. Received %v but handling %v",
				len(args.GetElement()), minArgs)
		}
	}

	jsonIndex := 0
	for i := startIndex; i < endIndex; i++ {
		valueArg, err := decodeArgAsNewValue(
			args.GetElement()[jsonIndex], handlerType.In(i))
		if err != nil {
			return nil, err
		}
		valueArgs = append(valueArgs, valueArg)
		jsonIndex++
	}

	if handlerType.IsVariadic() {
		variadicType := handlerType.In(endIndex).Elem()
		for ; jsonIndex < len(args.GetElement()); jsonIndex++ {
			valueArg, err := decodeArgAsNewValue(
				args.GetElement()[jsonIndex], variadicType)
			if err != nil {
				return nil, err
			}
			valueArgs = append(valueArgs, valueArg)
		}
	}

	return valueArgs, nil
}

func decodeArgAsValue(
	el *core.JSON, retObj interface{}) error {
	// TODO: This is a hack. We encode into actual JSON and then let the json
	//       library populate the result for us. This hurts performance quite
	//       a lot and should be improved with proper decoding.
	jsonEnc, err := json.Marshal(buildDataForJSON(el))
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonEnc, retObj)
}

func decodeArgAsNewValue(
	el *core.JSON, elType reflect.Type) (reflect.Value, error) {
	// TODO: This is a hack. We encode into actual JSON and then let the json
	//       library populate the result for us. This hurts performance quite
	//       a lot and should be improved with proper decoding.
	jsonEnc, err := json.Marshal(buildDataForJSON(el))
	if err != nil {
		return reflect.ValueOf(nil), err
	}
	dest := reflect.New(elType).Interface()
	err = json.Unmarshal(jsonEnc, dest)
	if err != nil {
		return reflect.ValueOf(nil), err
	}

	retValue := reflect.ValueOf(dest)
	if reflect.TypeOf(dest).Elem().Kind() != reflect.Struct {
		// Don't use pointer for non-structs.
		retValue = retValue.Elem()
	}
	return retValue, nil
}

func buildDataForJSON(el *core.JSON) interface{} {
	if el.GetJsonValueOneof() == nil {
		return nil
	}
	switch value := el.GetJsonValueOneof().(type) {
	case *core.JSON_JsonString:
		return value.JsonString
	case *core.JSON_JsonNumber:
		return value.JsonNumber
	case *core.JSON_JsonBool:
		return value.JsonBool
	case *core.JSON_JsonObject:
		mapData := make(map[string]interface{})
		for _, prop := range value.JsonObject.GetProperty() {
			mapData[prop.Name] = buildDataForJSON(prop.GetValue())
		}
		return mapData
	case *core.JSON_JsonArray:
		elems := value.JsonArray.GetElement()
		mapData := make([]interface{}, len(elems))
		for index, value := range elems {
			mapData[index] = buildDataForJSON(value)
		}
		return mapData
	default:
		panic("Invalid JSON value")
	}
}
