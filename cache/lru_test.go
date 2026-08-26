package cache

import (
	"context"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test LRU cache utils", func() {
	It("should return false for isExpired() when duration is zero", func() {
		c := NewLRUCache("test", 0)
		Expect(c.IsExpired()).Should(BeFalse())
	})

	It("should return false for isExpired() before duration elapses", func() {
		c := NewLRUCache("test", 10*time.Hour)
		Expect(c.IsExpired()).Should(BeFalse())
	})

	It("should return true for isExpired() after duration elapses", func() {
		c := NewLRUCache("test", time.Millisecond*100)
		time.Sleep(200 * time.Millisecond)
		Expect(c.IsExpired()).Should(BeTrue())
	})

	It("test lru cache store basic put and get", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{})
		Expect(err).Should(BeNil())

		store.Put("test", "test data", time.Second*2)
		store.Put("test2", "test data", 0)
		store.Put("test3", "test data", -1)
		time.Sleep(3 * time.Second)

		value, found := store.Get("test")
		Expect(value).Should(BeZero())
		Expect(found).Should(BeFalse())

		value, found = store.Get("test2")
		Expect(value).Should(Equal("test data"))
		Expect(found).Should(BeTrue())

		value, found = store.Get("test3")
		Expect(value).Should(Equal("test data"))
		Expect(found).Should(BeTrue())
	})

	It("test lru cache store delete key", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{})
		Expect(err).Should(BeNil())

		store.Put("test", "test data", time.Minute*2)
		store.Delete("test")
		value, found := store.Get("test")
		Expect(value).Should(BeZero())
		Expect(found).Should(BeFalse())
	})

	It("test lru cache store with multiple keys", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, int](ctx, Options[string, int]{})
		Expect(err).Should(BeNil())

		for i := 0; i < 100; i++ {
			key := "key-" + strconv.Itoa(i)
			store.Put(key, i, 0)
		}

		for i := 0; i < 100; i++ {
			key := "key-" + strconv.Itoa(i)
			value, found := store.Get(key)
			Expect(found).Should(BeTrue())
			Expect(value).Should(Equal(i))
		}
	})

	It("treats zero max size as unlimited", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{MaxSize: 0})
		Expect(err).Should(BeNil())

		for i := 0; i < 1001; i++ {
			store.Put(strconv.Itoa(i), "value", 0)
		}

		for i := 0; i < 1001; i++ {
			value, found := store.Get(strconv.Itoa(i))
			Expect(found).Should(BeTrue())
			Expect(value).Should(Equal("value"))
		}
	})

	It("treats zero max bytes as unlimited", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{MaxBytes: 0, SizeOf: func(_ string, value string) int64 {
			return int64(len(value))
		}})
		Expect(err).Should(BeNil())

		store.Put("first", "value", 0)
		store.Put("second", "value2", 0)

		value, found := store.Get("first")
		Expect(found).Should(BeTrue())
		Expect(value).Should(Equal("value"))

		value, found = store.Get("second")
		Expect(found).Should(BeTrue())
		Expect(value).Should(Equal("value2"))
	})

	It("test lru cache store overwrite value", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{})
		Expect(err).Should(BeNil())

		store.Put("rw", "v1", time.Second)
		value, found := store.Get("rw")
		Expect(found).Should(BeTrue())
		Expect(value).Should(Equal("v1"))

		store.Put("rw", "v2", time.Second)
		value, found = store.Get("rw")
		Expect(found).Should(BeTrue())
		Expect(value).Should(Equal("v2"))
	})

	It("test lru cache store rejects value exceeding max memory", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{
			MaxBytes: 1,
			SizeOf: func(_ string, value string) int64 {
				return int64(len(value))
			},
		})
		Expect(err).Should(BeNil())

		store.Put("big", "abc", 0)
		value, found := store.Get("big")
		Expect(value).Should(BeZero())
		Expect(found).Should(BeFalse())
	})

	It("test lru cache store evicts oldest entry when memory is full", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{
			MaxBytes: 1,
			SizeOf: func(_ string, value string) int64 {
				return int64(len(value))
			},
		})
		Expect(err).Should(BeNil())

		store.Put("first", "a", 0)
		value, found := store.Get("first")
		Expect(found).Should(BeTrue())
		Expect(value).Should(Equal("a"))

		store.Put("second", "b", 0)

		value, found = store.Get("first")
		Expect(found).Should(BeFalse())
		Expect(value).Should(BeZero())

		value, found = store.Get("second")
		Expect(found).Should(BeTrue())
		Expect(value).Should(Equal("b"))
	})

	It("test lru cache store evicts oldest entry when max size is reached", func() {
		evicted := make(chan struct {
			key    string
			value  string
			reason EvictionReason
		}, 1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{
			MaxSize: 1,
			OnEvict: func(key string, value string, reason EvictionReason) {
				evicted <- struct {
					key    string
					value  string
					reason EvictionReason
				}{key: key, value: value, reason: reason}
			},
		})
		Expect(err).Should(BeNil())

		store.Put("first", "a", 0)
		store.Put("second", "b", 0)

		value, found := store.Get("first")
		Expect(found).Should(BeFalse())
		Expect(value).Should(BeZero())

		select {
		case evictedItem := <-evicted:
			Expect(evictedItem.key).Should(Equal("first"))
			Expect(evictedItem.value).Should(Equal("a"))
			Expect(evictedItem.reason).Should(Equal(EvictCapacity))
		default:
			Fail("expected eviction callback to be invoked")
		}
	})

	It("test lru cache store get on missing key", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{})
		Expect(err).Should(BeNil())

		value, found := store.Get("nonexistent")
		Expect(value).Should(BeZero())
		Expect(found).Should(BeFalse())
	})

	It("test lru cache store context cancellation stops sweep goroutine", func() {
		ctx, cancel := context.WithCancel(context.Background())
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{})
		Expect(err).Should(BeNil())

		store.Put("key", "value", 0)
		cancel()

		// After cancellation the store should still serve already-cached data
		value, found := store.Get("key")
		Expect(found).Should(BeTrue())
		Expect(value).Should(Equal("value"))
	})

	It("purge removes all entries", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{
			MaxBytes: 100,
			SizeOf:   func(_ string, value string) int64 { return int64(len(value)) },
		})
		Expect(err).Should(BeNil())

		store.Put("a", "hello", 0)
		store.Put("b", "world", 0)
		store.Purge()

		_, found := store.Get("a")
		Expect(found).Should(BeFalse())
		_, found = store.Get("b")
		Expect(found).Should(BeFalse())
	})

	It("CurrentBytes tracks memory across puts, deletes, and purge", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{
			MaxBytes: 100,
			SizeOf:   func(_ string, value string) int64 { return int64(len(value)) },
		})
		Expect(err).Should(BeNil())

		Expect(store.CurrentBytes()).Should(BeZero())

		store.Put("a", "hello", 0) // 5 bytes
		Expect(store.CurrentBytes()).Should(Equal(int64(5)))

		store.Put("b", "hi", 0) // 2 bytes
		Expect(store.CurrentBytes()).Should(Equal(int64(7)))

		store.Delete("a")
		Expect(store.CurrentBytes()).Should(Equal(int64(2)))

		store.Purge()
		Expect(store.CurrentBytes()).Should(BeZero())
	})

	It("sweep goroutine evicts expired entries with EvictTTL reason", func() {
		evicted := make(chan EvictionReason, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store, err := NewLRUStore[string, string](ctx, Options[string, string]{
			SweepInterval: 50 * time.Millisecond,
			OnEvict: func(_ string, _ string, reason EvictionReason) {
				select {
				case evicted <- reason:
				default:
				}
			},
		})
		Expect(err).Should(BeNil())

		store.Put("k", "v", 30*time.Millisecond)
		time.Sleep(200 * time.Millisecond) // let sweep fire at least once after TTL expires

		select {
		case reason := <-evicted:
			Expect(reason).Should(Equal(EvictTTL))
		case <-time.After(time.Second):
			Fail("timed out waiting for sweep to evict with EvictTTL")
		}
	})
})
