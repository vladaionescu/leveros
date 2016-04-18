package leverutil

import (
	"fmt"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
)

var (
	cacheLogger = GetLogger(PackageName, "cache")
)

// Cache is a data structure that keeps in-memory instances of objects for
// a specified amount of time after their last use.
type Cache struct {
	expiry      time.Duration
	constructor func(string) (interface{}, error)
	destructor  func(interface{})

	lock        sync.Mutex
	data        map[string]*cacheEntry
	lastUsedMap *treemap.Map // lastUsed (unix nano) -> list of keys
}

type cacheEntry struct {
	element  interface{}
	err      error
	lastUsed int64
	lock     sync.RWMutex
}

// NewCache creates a new Cache object.
func NewCache(
	expiry time.Duration,
	constructor func(string) (interface{}, error),
	destructor func(interface{})) *Cache {
	return &Cache{
		lastUsedMap: treemap.NewWith(int64Comparator),
		expiry:      expiry,
		constructor: constructor,
		destructor:  destructor,
		data:        make(map[string]*cacheEntry),
	}
}

// Get returns either a cached instance with provided key or a newly
// constructed one, if an instance is not found.
func (cache *Cache) Get(key string) (interface{}, error) {
	cache.lock.Lock()
	entry, ok := cache.data[key]
	if ok {
		cache.keepAliveInternal(key)
		cache.lock.Unlock()
		entry.lock.RLock() // Will block if currently constructing.
		defer entry.lock.RUnlock()
		return entry.element, nil
	}

	// Add entry to cache (before constructing).
	entry = &cacheEntry{
		lastUsed: time.Now().UnixNano(),
	}
	treemapInsert(cache.lastUsedMap, entry.lastUsed, key)
	cache.data[key] = entry
	cache.maybeScheduleExpire(entry.lastUsed)
	cache.lock.Unlock()

	// Construct outside cache's lock.
	entry.lock.Lock()
	element, err := cache.constructor(key)
	entry.element = element
	entry.err = err
	entry.lock.Unlock()

	// Remove from cache if construction failed.
	if err != nil {
		cache.lock.Lock()
		delete(cache.data, key)
		treemapRemove(cache.lastUsedMap, entry.lastUsed, key)
		cache.lock.Unlock()
	}

	return entry.element, entry.err
}

// GetExisting returns a cached instance with provided key, if it exists.
func (cache *Cache) GetExisting(key string) (interface{}, bool) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	entry, ok := cache.data[key]
	if ok {
		cache.keepAliveInternal(key)
		entry.lock.RLock() // Will block if currently constructing.
		defer entry.lock.RUnlock()
		return entry.element, true
	}
	return nil, false
}

// KeepAlive can be used to prevent expiry of instance with provided key.
// Returns false if instance does not exist.
func (cache *Cache) KeepAlive(key string) bool {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	return cache.keepAliveInternal(key)
}

// Destroy removes an instance from the Cache and calls the destructor for it.
func (cache *Cache) Destroy(key string) bool {
	cache.lock.Lock()
	entry, ok := cache.data[key]
	if !ok {
		cache.lock.Unlock()
		return false
	}
	delete(cache.data, key)
	treemapRemove(cache.lastUsedMap, entry.lastUsed, key)
	cache.lock.Unlock()

	cache.destroyEntry(entry)
	return true
}

func (cache *Cache) destroyEntry(entry *cacheEntry) {
	entry.lock.Lock() // Will block if currently constructing.
	defer entry.lock.Unlock()
	if entry.element != nil {
		cache.destructor(entry.element)
		entry.element = nil
		entry.err = fmt.Errorf("Was destructed")
	}
}

func (cache *Cache) keepAliveInternal(key string) bool {
	entry, ok := cache.data[key]
	if !ok {
		return false
	}
	treemapRemove(cache.lastUsedMap, entry.lastUsed, key)
	entry.lastUsed = time.Now().UnixNano()
	treemapInsert(cache.lastUsedMap, entry.lastUsed, key)
	cache.maybeScheduleExpire(entry.lastUsed)
	return true
}

func (cache *Cache) maybeScheduleExpire(lastUsed int64) {
	if cache.lastUsedMap.Left().(int64) != lastUsed {
		// Already scheduled.
		return
	}
	value, _ := cache.lastUsedMap.Get(lastUsed)
	if value != nil && len(value.([]string)) > 1 {
		// Already scheduled.
		return
	}
	cache.doExpire()
}

func (cache *Cache) doExpire() {
	for !cache.lastUsedMap.Empty() {
		lastUsed := cache.lastUsedMap.Left().(int64)
		expiryTime := time.Unix(0, lastUsed).Add(cache.expiry)
		if expiryTime.Before(time.Now()) {
			// Entry expired.
			value, _ := cache.lastUsedMap.Get(lastUsed)
			if value != nil {
				for _, key := range value.([]string) {
					entry := cache.data[key]
					delete(cache.data, key)
					// Run in parallel so as not to hold cache lock.
					go cache.destroyEntry(entry)
				}
			}
			cache.lastUsedMap.Remove(lastUsed)
		} else {
			// Not yet time. Schedule next.
			untilExpiry := expiryTime.Sub(time.Now())
			go func() {
				time.Sleep(untilExpiry)
				cache.lock.Lock()
				cache.doExpire()
				cache.lock.Unlock()
			}()
			break
		}
	}
}

func int64Comparator(a, b interface{}) int {
	aInt64 := a.(int64)
	bInt64 := b.(int64)
	switch {
	case aInt64 > bInt64:
		return 1
	case aInt64 < bInt64:
		return -1
	default:
		return 0
	}
}

func treemapInsert(theMap *treemap.Map, key int64, target string) {
	value, _ := theMap.Get(key)
	var entry []string
	if value == nil {
		entry = []string{target}
	} else {
		entry = value.([]string)
		entry = append(entry, target)
	}
	theMap.Put(key, entry)
}

func treemapRemove(theMap *treemap.Map, key int64, target string) {
	value, _ := theMap.Get(key)
	if value == nil {
		cacheLogger.Error("Could not find element in treemap")
		return
	}
	entry := value.([]string)
	foundIndex := -1
	for index, targetJ := range entry {
		if target == targetJ {
			foundIndex = index
			break
		}
	}
	if foundIndex == -1 {
		cacheLogger.Error("Could not find element in treemap")
		return
	}
	entry = append(entry[:foundIndex], entry[foundIndex+1:]...)
	theMap.Put(key, entry)
}
