// Package singleflight provides a mechanism to suppress duplicate Get() calls
// for a given key.
package singleflight

import (
	"sync"

	"github.com/gebn/ttlcache/pkg/lifetime"
)

// call is an in-progress or completed Getter invocation.
type call struct {
	wg       sync.WaitGroup
	data     []byte
	lifetime lifetime.Lifetime
	err      error
}

type Group struct {
	// mu protects access to calls.
	mu sync.Mutex

	// calls stores in progress and completed invocations by key.
	calls map[string]*call
}

func NewGroup(concurrentCalls uint) *Group {
	return &Group{
		calls: make(map[string]*call, concurrentCalls),
	}
}

// Do executes and returns the results of the given function, making sure that
// only one execution is in-flight for a given key at a time. If a call comes in
// for the same key, the duplicate caller waits for the original to complete and
// receives the same result. The first parameter indicates whether the call was
// deduplicated, and will be true for all but the first concurrent caller.
func (g *Group) Do(key string, f func() ([]byte, lifetime.Lifetime, error)) (bool, []byte, lifetime.Lifetime, error) {
	// could use a RWMutex here, but we assume concurrent calls are rare
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		// call in progress; block until it completes then return the result
		g.mu.Unlock()
		c.wg.Wait()
		return true, c.data, c.lifetime, c.err
	}

	c := &call{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	c.data, c.lifetime, c.err = f()
	c.wg.Done()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return false, c.data, c.lifetime, c.err
}
