// Package lru implements a specialised least recently used cache.
package lru

import (
	"container/list"
	"sync"

	"github.com/gebn/ttlcache/pkg/lifetime"
)

// value holds the data and lifetime of a loaded key. This is logically the
// successful result of an OriginLoader.Load() call. It is a value type within
// the cache.
type value struct {
	// data is allowed to be nil.
	data     []byte
	lifetime lifetime.Lifetime
}

// entry represents a key and its value in the cache.
type entry struct {
	key   string
	value value
}

// EvictionFunc is a function called with a cache entry's lifetime when it is
// removed due to the cache being at capacity, a new entry needing to be added,
// and this one being the one referenced longest ago. The lifetime must not be
// modified.
type EvictionFunc func(lifetime.Lifetime)

// Cache implements an LRU cache that evicts its oldest entry as necessary. It
// is safe for concurrent access. The key is always a string; the value can be
// any type. Instances are regarded as a low-level primitive, so there is no
// built-in monitoring. This should be provided around the site of exported
// methods.
type Cache struct {

	// capacity is the maximum number of elements that the cache can hold. If
	// the cache reaches capacity, Put()s will result in eviction of the entry
	// touched longest ago.
	capacity uint

	// mu is used to synchronise access to internal state. As this is needed to
	// do pretty much anything, this struct is effectively single-threaded.
	mu sync.Mutex

	// onEvict is called with each evicted element. This happens as part of a
	// Put() before adding new keys (if the key already exists and is only an
	// update, no entry is evicted). This function is not called for pruned
	// elements.
	onEvict EvictionFunc

	// ll is a doubly-linked list containing pointers to entry structs. This
	// lets us efficiently move elements to the front of the list when they are
	// referenced. The last element will be evicted next.
	ll *list.List

	// cache provides constant time access to the doubly-linked list, providing
	// map semantics.
	cache map[string]*list.Element
}

// NewCache creates a new LRU cache with a maximum number of entries, and
// function to call when elements are evicted. The capacity can be 0, however a
// function must be provided.
//
// We generally care about bytes rather than the number of entries, as that's
// the underlying constraint, however we do not know how much memory is taken up
// by our internal data structures, and value may be owned elsewhere (so us
// storing a pointer to 24 bytes does not mean 24 bytes would've been saved if
// it wasn't in our cache). As a result, we delegate to the implementer, and use
// a metric that is perhaps less directly useful, but at least correct.
func NewCache(capacity uint, onEvict EvictionFunc) *Cache {
	if onEvict == nil {
		// this is not a general-purpose LRU - we always want an eviction
		// function in order to provide metrics about each cache
		panic("eviction function must be specified")
	}
	return &Cache{
		capacity: capacity,
		onEvict:  onEvict,
		ll:       list.New(),
		cache:    make(map[string]*list.Element, capacity),
	}
}

// Get retrieves the value stored under a key. If the key does not exist, the
// bool will be false, in which case other return values should be ignored. A
// key may contain nil data. The returned values must not be modified.
func (c *Cache) Get(key string) ([]byte, lifetime.Lifetime, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.cache[key]; ok {
		c.ll.MoveToFront(elem)
		v := elem.Value.(*entry).value
		return v.data, v.lifetime, true
	}
	return nil, lifetime.Zero, false
}

// Put adds, or updates the value stored under, a key. It returns whether the
// key already existed in the cache. Data may be nil.
func (c *Cache) Put(key string, data []byte, lt lifetime.Lifetime) bool {
	if c.capacity == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	v := value{
		data:     data,
		lifetime: lt,
	}
	if elem, ok := c.cache[key]; ok {
		c.ll.MoveToFront(elem)
		elem.Value.(*entry).value = v
		return true
	}
	if uint(c.ll.Len())+1 > c.capacity {
		// remove oldest first
		c.evictOldest()
	}
	elem := c.ll.PushFront(&entry{
		key:   key,
		value: v,
	})
	c.cache[key] = elem
	return false
}

// evictOldest removes the provided element from the cache. This must be called
// while holding the mutex.
func (c *Cache) evictOldest() {
	e := c.ll.Back()
	c.ll.Remove(e)
	entry := e.Value.(*entry)
	delete(c.cache, entry.key)
	c.onEvict(entry.value.lifetime)
}

// PruneExpired scans the cache and removes elements whose lifetime has expired.
// Note EvictionFunc is not called.
//
// This function incurs a latency hit on other access. On the surface we want a
// larger number of smaller caches to mitigate the effect, however this leads to
// more network requests between peers, which are orders of magnitude slower.
func (c *Cache) PruneExpired() uint {
	// we could have taken a predicate here, however we only need one
	// implementation, and this is an internal package
	c.mu.Lock()
	defer c.mu.Unlock()
	expired := uint(0)
	// we bypass the LRU logic moving elements around
	for key, elem := range c.cache {
		if value := elem.Value.(*entry).value; value.lifetime.Expired() {
			c.ll.Remove(elem) // O(1), which is handy
			delete(c.cache, key)
			expired++
		}
	}
	return expired
}
