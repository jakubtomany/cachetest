package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/google/uuid"
)

type cacheItem[T any] struct {
	lock  sync.Mutex
	value *itemValue[T]
}

type itemValue[T any] struct {
	value T
}

type reader[T any] func() T

func readUUID(cnt *int, sleep time.Duration) func() string {
	return func() string {
		*cnt++
		time.Sleep(sleep)
		return uuid.NewString()
	}
}

func newLockInItemCache[T any]() *lockInItemCache[T] {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		panic(err)
	}
	return &lockInItemCache[T]{
		cache: cache,
	}
}

type lockInItemCache[T any] struct {
	cache *ristretto.Cache
}

func (c *lockInItemCache[T]) LoadOrStore(key int, read reader[T]) T {
	var item *cacheItem[T]

	entry, ok := c.cache.Get(key)
	if !ok {
		// create cache entry with lock for the key
		c.cache.SetWithTTL(key, &cacheItem[T]{}, 1, ttl)
		c.cache.Wait()
		entry, _ = c.cache.Get(key)
	}

	item = entry.(*cacheItem[T])
	if item.value != nil {
		return item.value.value
	}

	item.lock.Lock()
	defer item.lock.Unlock()

	// make sure the value has not been set in the cache yet
	entry, _ = c.cache.Get(key)
	item = entry.(*cacheItem[T])
	if item.value != nil {
		return item.value.value
	}

	// read the value with reader and store it in the cache
	item.value = &itemValue[T]{
		value: read(),
	}
	c.cache.SetWithTTL(key, item, 1, ttl)
	c.cache.Wait()

	return item.value.value
}

func newSyncMapCache[T any]() *syncMapCache[T] {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		panic(err)
	}
	return &syncMapCache[T]{
		cache: cache,
	}
}

type syncMapCache[T any] struct {
	cache *ristretto.Cache
	locks sync.Map
}

func (c *syncMapCache[T]) LoadOrStore(key int, read reader[T]) T {
	val, ok := c.cache.Get(key)
	if ok {
		return val.(*itemValue[T]).value
	}

	anyLock, _ := c.locks.LoadOrStore(key, &sync.Mutex{})
	lock := anyLock.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	val, ok = c.cache.Get(key)
	if ok {
		return val.(T)
	}

	value := &itemValue[T]{value: read()}
	c.cache.SetWithTTL(key, value, 1, ttl)
	c.cache.Wait()

	return value.value
}

const (
	rounds   = 100
	routines = 1000
	reps     = 10000
	ttl      = 2 * time.Second
)

func main() {
	var wg sync.WaitGroup

	for _, slp := range []time.Duration{0, 10 * time.Millisecond, 100 * time.Millisecond, 300 * time.Millisecond} {
		fmt.Println("\n##################################")
		fmt.Println("Starting with sleep", slp)
		start := time.Now()

		itmLckCache := newLockInItemCache[string]()
		sncMapCache := newSyncMapCache[string]()

		var ilCacheDur time.Duration = 0
		var smCacheDur time.Duration = 0
		ilReadCnt := 0
		smReadCnt := 0

		for round := 0; round < rounds; round++ {
			//fmt.Println("\nRound ", round)

			roundStart := time.Now()
			for i := 0; i < routines; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					for j := 0; j < reps; j++ {
						itmLckCache.LoadOrStore(i, readUUID(&ilReadCnt, slp))
					}
				}(i)
			}
			wg.Wait()
			dur := time.Since(roundStart)
			ilCacheDur += dur
			//fmt.Println("lockInItemCache: ", dur)

			roundStart = time.Now()
			for i := 0; i < routines; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					for j := 0; j < reps; j++ {
						sncMapCache.LoadOrStore(i, readUUID(&smReadCnt, slp))
					}
				}(i)
			}
			wg.Wait()
			dur = time.Since(roundStart)
			smCacheDur += dur
			//fmt.Println("   syncMapCache: ", dur)
		}

		fmt.Println("\nlockInItemCache")
		fmt.Println(" number of reads:", ilReadCnt, "\n duration:", ilCacheDur)
		fmt.Println("\nsyncMapCache")
		fmt.Println(" number of reads:", smReadCnt, "\n duration:", smCacheDur)
		fmt.Println("\nTotal duration:", time.Since(start))
	}
}
