package cache

import (
	"time"
)

// Cache interface defines the methods for a cache implementation.
type Cache[K comparable] interface {
	// Get retrieves a value from the cache by key.
	Get(key K) (value interface{}, found bool)

	// Put adds a value to the cache with the specified key and expiration time.
	Put(key K, value interface{}, expiration time.Duration)

	// Delete removes a value from the cache by key.
	Delete(key K)
}

// LRUCache interface defines the methods for a Least Recently Used (LRU) cache implementation.
type LRUCache[K comparable, V any] interface {
	// Get retrieves a value from the cache by key.
	Get(key K) (value V, found bool)

	// Put adds a value to the cache with the specified key and expiration time.
	Put(key K, value V, expiration time.Duration)

	// Delete removes a value from the cache by key.
	Delete(key K)
}

// EvictionReason represents the reason for evicting an item from the cache.
type EvictionReason string

const (
	// EvictCapacity indicates that the item was evicted due to reaching the maximum capacity of the cache.
	EvictCapacity EvictionReason = "capacity"
	// EvictTTL indicates that the item was evicted due to exceeding its time-to-live (TTL).
	EvictTTL EvictionReason = "TTL"
	// EvictReplace indicates that the item was evicted due to being replaced by a new value.
	EvictReplace EvictionReason = "replace"
	// EvictDelete indicates that the item was evicted due to being explicitly deleted from the cache.
	EvictDelete EvictionReason = "delete"
	// EvictPurge indicates that the item was evicted due to a purge operation on the cache.
	EvictPurge EvictionReason = "purge"
)

// Options struct defines the configuration options for the LRU cache.
type Options[K comparable, V any] struct {
	// MaxSize defines the maximum number of items the cache can hold.
	MaxSize int // 0 = unlimited count
	// MaxBytes defines the maximum bytes the cache can hold.
	MaxBytes int64 // 0 = unlimited bytes
	// SizeOf is a function that calculates the byte usage of the value
	SizeOf func(key K, value V) int64 // required if MaxBytes > 0
	// OnEvict is a callback function that is called when an item is evicted from the cache.
	OnEvict func(key K, value V, reason EvictionReason)
	// SweepInterval defines the interval at which the cache will sweep for expired items.
	SweepInterval time.Duration
}
