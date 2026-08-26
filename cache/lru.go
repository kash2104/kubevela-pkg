package cache

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	hashicorp "github.com/hashicorp/golang-lru/v2"
)

const (
	// DefaultMemoryCacheSweepInterval is the default interval for sweeping expired items from the cache.
	DefaultMemoryCacheSweepInterval = time.Minute * 5
)

type lruCache[V any] struct {
	data           V
	cacheDuration  time.Duration
	startTime      time.Time
	memorySize     int64
	evictionReason EvictionReason
}

type evictionEvent[K comparable, V any] struct {
	key   K
	value *lruCache[V]
}

// NewLRUCache creates a new lruCache entry with the given value and TTL
func NewLRUCache[V any](data V, cacheDuration time.Duration) *lruCache[V] {
	lc := &lruCache[V]{data: data, cacheDuration: cacheDuration, startTime: time.Now(), evictionReason: EvictCapacity}

	return lc
}

// IsExpired checks if the cache entry's TTL has elapsed.
func (l *lruCache[V]) IsExpired() bool {
	if l.cacheDuration <= 0 {
		return false
	}
	return time.Now().After(l.startTime.Add(l.cacheDuration))
}

// LRUStore is a thread-safe LRU cache implementation that supports expiration and memory size limits.
type LRUStore[K comparable, V any] struct {
	store *hashicorp.Cache[K, *lruCache[V]]
	// maximumMemory Maximum Memory byte size of the cache
	maximumMemory int64 // 0 = unlimited bytes
	// sizeOf computes memory usage for each key/value entry.
	sizeOf func(key K, value V) int64
	// sweepInterval controls how often expired entries are removed.
	sweepInterval time.Duration
	// onEvict callback when an entry is evicted.
	OnEvict func(key K, value V, reason EvictionReason)
	// currentMemory Memory used by the cache
	currentMemory int64
	// mu mutex for synchronizing access to the cache
	mu sync.Mutex
	// pendingEvicts holds the list of eviction events to be processed after releasing the lock
	pendingEvicts []evictionEvent[K, V]
}

// NewLRUStore creates a new LRUStore with the given options.
func NewLRUStore[K comparable, V any](ctx context.Context, opts Options[K, V]) (*LRUStore[K, V], error) {
	if opts.MaxSize <= 0 {
		// 0 = unlimited count: use the largest possible size so that only
		// the byte budget (MaxBytes) drives eviction.
		opts.MaxSize = math.MaxInt
	}

	if opts.MaxBytes < 0 {
		opts.MaxBytes = 0
	}

	if opts.MaxBytes > 0 && opts.SizeOf == nil {
		return nil, fmt.Errorf("SizeOf function must be provided when MaxBytes is greater than 0")
	}

	if opts.SweepInterval <= 0 {
		opts.SweepInterval = DefaultMemoryCacheSweepInterval
	}

	lc := &LRUStore[K, V]{
		maximumMemory: opts.MaxBytes,
		sizeOf:        opts.SizeOf,
		sweepInterval: opts.SweepInterval,
		OnEvict:       opts.OnEvict,
		currentMemory: 0,
	}

	lru, err := hashicorp.NewWithEvict(opts.MaxSize, func(key K, value *lruCache[V]) {
		if value != nil {
			atomic.AddInt64(&lc.currentMemory, -value.memorySize)
		}
		if lc.OnEvict != nil && value != nil {
			lc.pendingEvicts = append(lc.pendingEvicts, evictionEvent[K, V]{
				key:   key,
				value: value,
			})
		}
	})

	if err != nil {
		return nil, err
	}

	lc.store = lru

	go lc.run(ctx)

	return lc, nil

}

// run starts a goroutine that periodically sweeps the cache for expired items.
func (l *LRUStore[K, V]) run(ctx context.Context) {
	ticker := time.NewTicker(l.sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.mu.Lock()
			for _, key := range l.store.Keys() {
				val, ok := l.store.Peek(key)
				if ok && val != nil && val.IsExpired() {
					val.evictionReason = EvictTTL
					l.store.Remove(key)
				}
			}
			l.executeEviction()
		}
	}
}

// Get retrieves a value from the cache by key.
func (l *LRUStore[K, V]) Get(key K) (value V, found bool) {
	l.mu.Lock()
	defer l.executeEviction()
	lc, ok := l.store.Get(key)

	if !ok {
		return
	}

	if lc.IsExpired() {
		lc.evictionReason = EvictTTL
		l.store.Remove(key)
		return
	}

	return lc.data, true
}

// Put adds a value to the cache with the specified key and expiration time.
func (l *LRUStore[K, V]) Put(key K, value V, cacheDuration time.Duration) {
	l.mu.Lock()
	defer l.executeEviction()
	lc := NewLRUCache(value, cacheDuration)

	if l.sizeOf != nil {
		lc.memorySize = l.sizeOf(key, value)
	}

	if l.maximumMemory != 0 && lc.memorySize > l.maximumMemory {
		return
	}

	if prevValue, ok := l.store.Peek(key); ok && prevValue != nil {
		prevValue.evictionReason = EvictReplace
		l.store.Remove(key)
	}

	if l.maximumMemory != 0 {
		for atomic.LoadInt64(&l.currentMemory)+lc.memorySize > l.maximumMemory {
			_, _, ok := l.store.RemoveOldest()
			if !ok {
				// If there is nothing left to evict, refuse the write.
				return
			}
		}
	}

	// track bytes whenever sizeOf is provided, even if maximumMemory is 0.
	if l.sizeOf != nil {
		atomic.AddInt64(&l.currentMemory, lc.memorySize)
	}

	l.store.Add(key, lc)

}

// Delete removes a value from the cache by key.
func (l *LRUStore[K, V]) Delete(key K) {
	l.mu.Lock()
	defer l.executeEviction()
	if val, ok := l.store.Peek(key); ok && val != nil {
		val.evictionReason = EvictDelete
		l.store.Remove(key)
	}
}

// CurrentBytes returns the current memory usage of the cache in bytes.
func (l *LRUStore[K, V]) CurrentBytes() int64 {
	return atomic.LoadInt64(&l.currentMemory)
}

// Purge removes all items from the cache.
func (l *LRUStore[K, V]) Purge() {
	l.mu.Lock()
	defer l.executeEviction()
	for _, key := range l.store.Keys() {
		if val, ok := l.store.Peek(key); ok && val != nil {
			val.evictionReason = EvictPurge
		}
	}
	l.store.Purge()
}

func (l *LRUStore[K, V]) executeEviction() {
	evicts := l.pendingEvicts
	l.pendingEvicts = nil
	l.mu.Unlock()

	for _, evict := range evicts {
		if l.OnEvict != nil && evict.value != nil {
			l.OnEvict(evict.key, evict.value.data, evict.value.evictionReason)
		}
	}
}
