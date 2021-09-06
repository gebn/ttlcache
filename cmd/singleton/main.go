package main

// This demonstrates the simplest cache instance. In practice you wouldn't use
// ttlcache in this way, however it is useful for testing.

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gebn/ttlcache"
	"github.com/gebn/ttlcache/pkg/lifetime"
	"github.com/hashicorp/memberlist"
)

func main() {
	if err := app(context.Background()); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func app(ctx context.Context) error {
	base := ttlcache.NewBase(&ttlcache.BaseOpts{
		Name:                       "name",
		PeerPicker:                 PeerPicker{},
		AuthoritativeCacheCapacity: 5_000_000,
		HotCacheCapacity:           5_000,
		ParallelRequests:           50,
	})
	cache := base.Configure(&ttlcache.ConfigureOpts{
		OriginLoader: ttlcache.OriginLoaderFunc(func(_ context.Context, key string) ([]byte, lifetime.Lifetime, error) {
			log.Printf("origin load for %v", key)
			return []byte(key + "-value"), lifetime.New(time.Minute), nil
		}),
	})
	for i := 0; i < 5; i++ {
		d, lt, err := cache.Get(ctx, "a")
		if err != nil {
			return err
		}
		log.Printf("%v, expires %v", string(d), lt)
	}
	return nil
}

type PeerPicker struct{}

func (p PeerPicker) PickPeer(key string) *memberlist.Node {
	return nil
}
