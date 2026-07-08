package cache

import "time"

// Cache interface defines the methods for a cache implementation.
type Cache interface {
	// Get retrieves a value from the cache by key.
	Get(key interface{}) (value interface{})

	// Put adds a value to the cache with the specified key and expiration time.
	Put(key, value interface{}, expiration time.Duration)

	// Delete removes a value from the cache by key.
	Delete(key interface{})
}
