package ttlcache

import (
	"context"

	"github.com/gebn/ttlcache/pkg/lifetime"
)

// OriginLoader knows how to retrieve the values for keys unknown to the cache,
// from the original source. This is the key interface implemented by users of
// the library. An instance is provided when configuring a particular base, so
// the cache name is not passed. Implementations must be safe for concurrent
// use.
type OriginLoader interface {

	// SourceLoader was considered for this, however source is relative; the
	// point is the source is the *original* source - not another peer.

	// Load produces the value for a key, along with its lifetime. Zero-length
	// data must be returned as nil. This method may be called for any key on
	// any instance in the cluster at any time, so should return a
	// deterministic, consistent value. This is similar to Sink in groupcache.
	//
	// Note the expiry of the context is defined when configuring a base cache,
	// not by incoming external requests. We deduplicate loads, so want to avoid
	// the situation where one one impatient client causes everyone waiting on
	// the same key to receive an error.
	//
	// It is strongly recommended to stagger TTL expiration to avoid a
	// thundering herd on the origin, especially if it is remote. To achieve
	// this, add some jitter to the TTL to spread them over the longest period
	// that can be tolerated. Remember the jitter must be deterministic, so use
	// a value derived from the key, e.g. crc32, rather than math/rand.
	//
	// Values are cached amongst peers, and not reloaded until their TTL has
	// been reached. The TTL returned can be retrospectively modified via an
	// override, which allows decreasing the value, however this is only
	// intended for emergencies in very limited scenarios. Infinite TTLs can be
	// emulated by returning lifetime.New(lifetime.MaxDuration), however if you
	// do not want TTLs, you may want to consider another library.
	//
	// Excluding failure scenarios, this will only be called once for a given
	// key, by the cluster member that owns that key. If that cluster member
	// dies, its keys will be lazily re-retrieved as necessary. Concurrent
	// requests for a given key will only occur if the request to the owning
	// cluster member fails, in which case the requesters affected will each
	// retry this locally, possibly causing a small thundering herd. Due to
	// this, an error should only be returned when the key could not be
	// retrieved due to a transient error - not because they key does not (yet)
	// exist. In this case, it is recommended to return a static value with a
	// short TTL, which will prevent the cache from retrying for a period of
	// time. This method is responsible for implementing its own retries as
	// appropriate.
	Load(ctx context.Context, key string) ([]byte, lifetime.Lifetime, error)
}

// OriginLoaderFunc simplifies implementation of stateless OriginLoaders.
type OriginLoaderFunc func(ctx context.Context, key string) ([]byte, lifetime.Lifetime, error)

func (f OriginLoaderFunc) Load(ctx context.Context, key string) ([]byte, lifetime.Lifetime, error) {
	return f(ctx, key)
}
