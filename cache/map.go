package cache

import (
	"context"
	"sync"
	"time"
)

const (
	// DefaultSweepInterval Cache default sweep interval
	DefaultSweepInterval = time.Second * 3
)

// memoryCache memory cache, support time expired
type memoryCache struct {
	data          interface{}
	cacheDuration time.Duration
	startTime     time.Time
}

// NewMemoryCache new memory cache instance
func NewMemoryCache(data interface{}, cacheDuration time.Duration) *memoryCache {
	mc := &memoryCache{data: data, cacheDuration: cacheDuration, startTime: time.Now()}
	return mc
}

// IsExpired whether the cache data expires
func (m *memoryCache) IsExpired() bool {
	if m.cacheDuration <= 0 {
		return false
	}
	return time.Now().After(m.startTime.Add(m.cacheDuration))
}

// GetData get cache data
func (m *memoryCache) GetData() interface{} {
	return m.data
}

// MemoryCacheStore memory cache store
type MemoryCacheStore struct {
	store sync.Map
}

// NewMemoryCacheStore memory cache store
func NewMemoryCacheStore(ctx context.Context) *MemoryCacheStore {
	mcs := &MemoryCacheStore{
		store: sync.Map{},
	}
	go mcs.run(ctx)
	return mcs
}

// run start a goroutine to clear expired cache data
func (m *MemoryCacheStore) run(ctx context.Context) {
	ticker := time.NewTicker(DefaultSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.store.Range(func(key, value interface{}) bool {
				if value.(*memoryCache).IsExpired() {
					m.store.Delete(key)
				}
				return true
			})
		}
	}
}

// Get cache data from store, if cache data is expired, return nil
func (m *MemoryCacheStore) Get(key interface{}) (value interface{}) {
	mc, ok := m.store.Load(key)
	if ok && !mc.(*memoryCache).IsExpired() {
		return mc.(*memoryCache).GetData()
	}
	return nil
}

// Put cache data, if cacheDuration>0, store will clear data after timeout.
func (m *MemoryCacheStore) Put(key, value interface{}, cacheDuration time.Duration) {
	mc := NewMemoryCache(value, cacheDuration)
	m.store.Store(key, mc)
}

// Delete cache data from store
func (m *MemoryCacheStore) Delete(key interface{}) {
	m.store.Delete(key)
}
