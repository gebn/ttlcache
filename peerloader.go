package ttlcache

import (
	"context"

	"github.com/gebn/ttlcache/pkg/lifetime"

	"github.com/hashicorp/memberlist"
)

// PeerLoader knows how to retrieve values for keys unknown to the local
// instance from a peer. This is logically an inter-cache version of
// OriginLoader. An implementation must be provided when configuring a base; a
// reference one is available in the rpc package. Instances are scoped to a
// cache, but the implementation is provided with the cache object, so in
// practice it can be shared.
type PeerLoader interface {

	// Load requests a key from another peer, which will currently always be the
	// authoritative peer. This peer should call cache.Get(), which may result
	// in a load via via OriginLoader.
	Load(ctx context.Context, node *memberlist.Node, cache *Cache, key string) ([]byte, lifetime.Lifetime, error)
}

// PeerLoaderFunc simplifies implementation of stateless PeerLoaders.
type PeerLoaderFunc func(ctx context.Context, node *memberlist.Node, cache *Cache, key string) ([]byte, lifetime.Lifetime, error)

func (f PeerLoaderFunc) Load(ctx context.Context, node *memberlist.Node, cache *Cache, key string) ([]byte, lifetime.Lifetime, error) {
	return f(ctx, node, cache, key)
}
