package ttlcache

import (
	"context"

	"github.com/gebn/ttlcache/pkg/lifetime"

	"github.com/hashicorp/memberlist"
)

// PeerLoader knows how to retrieve values of keys unknown to the local
// instance from a peer. This is conceptually an intra-cache version of
// OriginLoader. An instance must be provided when configuring a base; a
// reference implementation is available in the rpc package. Instances are
// scoped to a cache, but the implementation is provided with the cache object,
// so in practice they can be shared amongst multiple caches. Implementations
// must be safe for concurrent use.
type PeerLoader interface {

	// Load requests a key from the specified peer, which will currently always
	// be the authoritative peer. This peer should call cache.Get(), which may
	// result in a load via the OriginLoader.
	Load(ctx context.Context, from *memberlist.Node, cache *Cache, key string) ([]byte, lifetime.Lifetime, error)
}

// PeerLoaderFunc simplifies implementation of stateless PeerLoaders.
type PeerLoaderFunc func(ctx context.Context, from *memberlist.Node, cache *Cache, key string) ([]byte, lifetime.Lifetime, error)

func (f PeerLoaderFunc) Load(ctx context.Context, from *memberlist.Node, cache *Cache, key string) ([]byte, lifetime.Lifetime, error) {
	return f(ctx, from, cache, key)
}
