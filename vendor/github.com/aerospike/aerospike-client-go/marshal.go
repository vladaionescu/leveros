// Copyright 2013-2016 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	aerospikeTag     = "as"
	aerospikeMetaTag = "asm"
	keyTag           = "key"
)

func valueToInterface(f reflect.Value, clusterSupportsFloat bool) interface{} {
	// get to the core value
	for f.Kind() == reflect.Ptr {
		if f.IsNil() {
			return nil
		}
		f = reflect.Indirect(f)
	}

	switch f.Kind() {
	case reflect.Uint64:
		return int64(f.Uint())
	case reflect.Float64, reflect.Float32:
		// support floats through integer encoding if
		// server doesn't support floats
		if clusterSupportsFloat {
			return f.Float()
		} else {
			return int(math.Float64bits(f.Float()))
		}
	case reflect.Struct:
		if f.Type().PkgPath() == "time" && f.Type().Name() == "Time" {
			return f.Interface().(time.Time).UTC().UnixNano()
		}
		return structToMap(f, clusterSupportsFloat)
	case reflect.Bool:
		if f.Bool() == true {
			return int64(1)
		}
		return int64(0)
	case reflect.Map:
		if f.IsNil() {
			return nil
		}

		newMap := make(map[interface{}]interface{}, f.Len())
		for _, mk := range f.MapKeys() {
			newMap[valueToInterface(mk, clusterSupportsFloat)] = valueToInterface(f.MapIndex(mk), clusterSupportsFloat)
		}

		return newMap
	case reflect.Slice, reflect.Array:
		if f.Kind() == reflect.Slice && f.IsNil() {
			return nil
		}

		// convert to primitives recursively
		newSlice := make([]interface{}, f.Len(), f.Cap())
		for i := 0; i < len(newSlice); i++ {
			newSlice[i] = valueToInterface(f.Index(i), clusterSupportsFloat)
		}

		return newSlice
	case reflect.Interface:
		if f.IsNil() {
			return nil
		}
		return f.Interface()
	default:
		return f.Interface()
	}
}

func fieldIsMetadata(f reflect.StructField) bool {
	meta := f.Tag.Get(aerospikeMetaTag)
	return strings.Trim(meta, " ") != ""
}

func fieldAlias(f reflect.StructField) string {
	alias := f.Tag.Get(aerospikeTag)
	if alias != "" {
		alias = strings.Trim(alias, " ")

		// if tag is -, the field should not be persisted
		if alias == "-" {
			return ""
		}
		return alias
	}
	return f.Name
}

func structToMap(s reflect.Value, clusterSupportsFloat bool) map[string]interface{} {
	if !s.IsValid() {
		return nil
	}

	// map tags
	cacheObjectTags(s)

	typeOfT := s.Type()
	numFields := s.NumField()

	var binMap map[string]interface{}
	for i := 0; i < numFields; i++ {
		// skip unexported fields
		if typeOfT.Field(i).PkgPath != "" {
			continue
		}

		if fieldIsMetadata(typeOfT.Field(i)) {
			continue
		}

		// skip transiet fields tagged `-`
		alias := fieldAlias(typeOfT.Field(i))
		if alias == "" {
			continue
		}

		binValue := valueToInterface(s.Field(i), clusterSupportsFloat)

		if binMap == nil {
			binMap = make(map[string]interface{}, numFields)
		}

		binMap[alias] = binValue
	}

	return binMap
}

func marshal(v interface{}, clusterSupportsFloat bool) []*Bin {
	s := reflect.Indirect(reflect.ValueOf(v).Elem())

	// map tags
	cacheObjectTags(s)

	numFields := s.NumField()
	bins := binPool.Get(numFields).([]*Bin)

	binCount := 0
	n := structToMap(s, clusterSupportsFloat)
	for k, v := range n {
		bins[binCount].Name = k

		bins[binCount].Value = NewValue(v)
		binCount++
	}

	return bins[:binCount]
}

type SyncMap struct {
	objectMappings map[reflect.Type]map[string]string
	objectFields   map[reflect.Type][]string
	objectTTLs     map[reflect.Type][]string
	objectGen      map[reflect.Type][]string
	mutex          sync.RWMutex
}

func (sm *SyncMap) setMapping(obj reflect.Value, mapping map[string]string, fields, ttl, gen []string) {
	objType := obj.Type()
	sm.mutex.Lock()
	sm.objectMappings[objType] = mapping
	sm.objectFields[objType] = fields
	sm.objectTTLs[objType] = ttl
	sm.objectGen[objType] = gen
	sm.mutex.Unlock()
}

func (sm *SyncMap) mappingExists(obj reflect.Value) bool {
	objType := obj.Type()
	sm.mutex.RLock()
	_, exists := sm.objectMappings[objType]
	sm.mutex.RUnlock()
	return exists
}

func (sm *SyncMap) getMapping(objType reflect.Type) map[string]string {
	sm.mutex.RLock()
	mapping := sm.objectMappings[objType]
	sm.mutex.RUnlock()
	return mapping
}

func (sm *SyncMap) getMetaMappings(obj reflect.Value) (ttl, gen []string) {
	if !obj.IsValid() {
		return nil, nil
	}
	objType := obj.Type()
	sm.mutex.RLock()
	ttl = sm.objectTTLs[objType]
	gen = sm.objectGen[objType]
	sm.mutex.RUnlock()
	return ttl, gen
}

func (sm *SyncMap) getFields(obj reflect.Value) []string {
	objType := obj.Type()
	sm.mutex.RLock()
	fields := sm.objectFields[objType]
	sm.mutex.RUnlock()
	return fields
}

var objectMappings = &SyncMap{
	objectMappings: map[reflect.Type]map[string]string{},
	objectFields:   map[reflect.Type][]string{},
	objectTTLs:     map[reflect.Type][]string{},
	objectGen:      map[reflect.Type][]string{},
}

func cacheObjectTags(obj reflect.Value) {
	// exit if already processed
	if objectMappings.mappingExists(obj) {
		return
	}

	for obj.Kind() == reflect.Ptr {
		if obj.IsNil() {
			return
		}
		obj = reflect.Indirect(obj)
	}

	mapping := map[string]string{}
	fields := []string{}
	ttl := []string{}
	gen := []string{}

	typeOfT := obj.Type()
	numFields := obj.NumField()
	for i := 0; i < numFields; i++ {
		f := typeOfT.Field(i)
		// skip unexported fields
		if f.PkgPath != "" {
			continue
		}

		tag := strings.Trim(f.Tag.Get(aerospikeTag), " ")
		tagM := strings.Trim(f.Tag.Get(aerospikeMetaTag), " ")

		if tag != "" && tagM != "" {
			panic(fmt.Sprintf("Cannot accept both data and metadata tags on the same attribute on struct: %s.%s", obj.Type().Name(), f.Name))
		}

		if tag != "-" && tagM == "" {
			if tag != "" {
				mapping[tag] = f.Name
				fields = append(fields, tag)
			} else {
				fields = append(fields, f.Name)
			}
		}

		if tagM == "ttl" {
			ttl = append(ttl, f.Name)
		} else if tagM == "gen" {
			gen = append(gen, f.Name)
		} else if tagM != "" {
			panic(fmt.Sprintf("Invalid metadata tag `%s` on struct attribute: %s.%s", tagM, obj.Type().Name(), f.Name))
		}
	}

	objectMappings.setMapping(obj, mapping, fields, ttl, gen)
}
