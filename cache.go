package ttlcache

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/gebn/ttlcache/internal/pkg/lru"
	"github.com/gebn/ttlcache/pkg/lifetime"

	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
)

// TTLOverrides represents the lifetimes to overlay onto a set of keys. Current
// lifetimes, including those already in local caches, will be capped at these
// values. This mechanism does not allow increasing TTL, as the background
// goroutines that periodically run through LRU caches are created along with
// the cache base, and do not have access to these overrides, so could expire
// values too soon. TTLs are checked and overridden during gets.
//
// Note, overriding a TTL to 0 will cause every non-concurrent get to hit the
// origin. To prematurely flush values representing unknown keys, it is instead
// recommended to set the TTL to a number of seconds, wait for all nodes with
// the key to reload it, then remove the override. As key payloads are opaque,
// we do not have the ability to only expire values that do not represent the
// desired value - everything for that key is affected.
//
// This type must not be modified once used to configure a base cache.
type TTLOverrides map[string]time.Duration

// ConfigureOpts contains reloadable configuration. It is passed to a Base cache
// to turn it into a useable one. Later, the original base can be reconfigured
// with new values for these parameters.
type ConfigureOpts struct {

	// OriginLoader is used to load values from the original source. This is
	// equivalent to Getter in groupcache, and is the user's responsibility to
	// implement.
	OriginLoader

	// OriginLoadTimeout is the time to allow on the context provided when
	// loading from the origin.
	OriginLoadTimeout time.Duration

	// PeerLoader is used to load values from peers. An reference implementation
	// of this is provided in the rpc package.
	PeerLoader

	// PeerLoadTimeout is the time to allow on the context provided when loading
	// from a peer. This is important, as we want to eventually fall back to a
	// local origin load.
	PeerLoadTimeout time.Duration

	// TTLOverrides is the set of key => TTL mappings to overlay onto keys. See
	// the type's documentation for more details.
	TTLOverrides

	// HotAddProbability is the likelihood of a value retrieved from a peer
	// being added to the hot LRU cache.
	HotAddProbability float64
}

// Cache represents a named cache instance, from which values can be retrieved.
type Cache struct {
	*Base
	originLoader      OriginLoader
	originLoadTimeout time.Duration
	peerLoader        PeerLoader
	peerLoadTimeout   time.Duration
	ttlOverrides      TTLOverrides
	hotAddCoefficient int
}

// Configure combines reloadable config with a base cache to form a useable one.
// See ConfigureOpts's documentation for a description of each field. This
// method can be called multiple times for a given base; all created caches
// remain valid.
func (b *Base) Configure(opts *ConfigureOpts) *Cache {
	b.baseConfigures.Inc()
	// it is unnecessary to initialise any time series here, because that only
	// requires the cache name, which is known when the base is created
	return &Cache{
		Base:              b,
		originLoader:      opts.OriginLoader,
		originLoadTimeout: opts.OriginLoadTimeout,
		peerLoader:        opts.PeerLoader,
		peerLoadTimeout:   opts.PeerLoadTimeout,
		ttlOverrides:      opts.TTLOverrides,
		hotAddCoefficient: int(1 / opts.HotAddProbability),
	}
}

// Get retrieves an element from the cache, returning the data along with its
// TTL. Our guarantee is to check the TTL before handing the value back, and
// reload the key if the TTL has been reached. Due to thread scheduling, we
// cannot promise to never return a TTL that has expired.  When retrieving a
// fresh value, we don't check the expiry, as then values may never get back to
// the user. Internal processing time is not deducted from the TTL.  It is not
// possible to know whether the value was fetched or already cached; that is
// only exposed in metrics in aggregate.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, lifetime.Lifetime, error) {
	timer := prometheus.NewTimer(c.Base.getDuration)
	defer timer.ObserveDuration()

	// check authoritative and hot outside singleflight - this is more
	// lightweight
	c.Base.authoritativeGets.Inc()
	if d, lt := c.tryLRU(key, c.Base.authoritative); d != nil {
		return d, lt, nil
	}
	c.Base.authoritativeMisses.Inc()
	c.Base.hotGets.Inc()
	if d, lt := c.tryLRU(key, c.Base.hot); d != nil {
		return d, lt, nil
	}
	c.Base.hotMisses.Inc()

	// we add new keys to the cache when they have never been seen before, or
	// previously been evicted. We may also update existing keys if their value
	// has expired (but not been evicted), or if an override has caused us to
	// ignore a still-valid TTL and re-fetch the value

	// we don't have a valid value for the key, so now lock the key to retrieve
	// it and add it to the correct LRU cache
	c.Base.getDedupeAttempts.Inc()
	deduped, data, lt, err := c.Base.flight.Do(key, func() ([]byte, lifetime.Lifetime, error) {
		// we must re-check the authoritative and hot caches due to an
		// unfortunate goroutine scheduling
		c.Base.authoritativeGets.Inc()
		if d, lt := c.tryLRU(key, c.Base.authoritative); d != nil {
			return d, lt, nil
		}
		// it may have been a hit, but a TTL override caused it to effectively
		// be a miss
		c.Base.authoritativeMisses.Inc()
		c.Base.hotGets.Inc()
		if d, lt := c.tryLRU(key, c.Base.hot); d != nil {
			return d, lt, nil
		}
		c.Base.hotMisses.Inc()

		// still nothing - we need to load the key either from a peer, or
		// ourselves
		node := c.Base.peerPicker.PickPeer(key)
		if node != nil {
			// owned by another peer
			ctx, cancel := context.WithTimeout(ctx, c.peerLoadTimeout)
			d, lt, err := c.peerLoad(ctx, node, key)
			cancel()
			if err == nil {
				c.maybeHotCache(key, d, lt)
				return d, c.capLifetime(key, lt), nil
			}
			log.Printf("peer load failure: %v", err)
			// peer may have died between pick and request; if we wanted to be
			// more aggressive about deduplicating loads, we could re-pick and
			// try again before falling back to loading ourselves
			c.Base.peerLoadFailures.Inc()
		}

		// owned by us, or peer load failure. Note we no longer care about the
		// client for this request - we want to load the key successfully; at
		// some point we could potentially cap this to the current request but
		// put the key on a queue to attempt to fill in the background with a
		// longer timeout, but that feels like overengineering.
		ctx, cancel := context.WithTimeout(context.Background(), c.originLoadTimeout)
		d, lt, err := c.originLoad(ctx, key)
		cancel()
		if err != nil {
			c.Base.originLoadFailures.Inc()
			c.Base.getFailures.Inc()
			return nil, lifetime.Zero, err
		}
		if node == nil {
			// we are authoritative; always cache if non-zero TTL
			if lt.TTL != 0 {
				c.Base.authoritative.Put(key, d, lt)
				c.Base.authoritativePuts.Inc()
			}
		} else {
			// zero duration check done for us
			c.maybeHotCache(key, d, lt)
		}
		return d, c.capLifetime(key, lt), nil
	})
	if !deduped {
		c.Base.getDedupeFailures.Inc()
	}
	return data, lt, err
}

func (c *Cache) capLifetime(key string, lt lifetime.Lifetime) lifetime.Lifetime {
	if override, ok := c.ttlOverrides[key]; ok {
		return lt.Cap(override)
	}
	return lt
}

func (c *Cache) tryLRU(key string, lruc *lru.Cache) ([]byte, lifetime.Lifetime) {
	d, lt, ok := lruc.Get(key)
	if !ok {
		return nil, lifetime.Zero
	}

	// if lt.Expired() here, an override could only make it "more" expired. We
	// don't remove it from the cache as a remove followed by an update (we
	// assume we will successfully retrieve the new value) is more expensive
	// than an update in place.

	// adjust for override if one is configured for the key
	lt = c.capLifetime(key, lt)
	if lt.Expired() {
		// possibly due only to the override in place
		return nil, lifetime.Zero
	}

	// overridden TTLs are not updated in the LRU caches - we overlay them after
	// items are retrieved. When the override is removed, the original TTL
	// becomes visible again. As an override can only shorten a TTL, this means
	// we don't prematurely expire what would otherwise be valid values - we
	// assume overrides are shortlived.
	return d, lt
}

func (c *Cache) peerLoad(ctx context.Context, node *memberlist.Node, key string) ([]byte, lifetime.Lifetime, error) {
	timer := prometheus.NewTimer(c.Base.peerLoadDuration)
	defer timer.ObserveDuration()
	return c.peerLoader.Load(ctx, node, c, key)
}

func (c *Cache) originLoad(ctx context.Context, key string) ([]byte, lifetime.Lifetime, error) {
	timer := prometheus.NewTimer(c.Base.originLoadDuration)
	defer timer.ObserveDuration()
	return c.originLoader.Load(ctx, key)
}

// maybeHotCache may add a new entry to the hot LRU cache.
func (c *Cache) maybeHotCache(key string, d []byte, lt lifetime.Lifetime) {
	if lt.TTL != 0 && rand.Intn(c.hotAddCoefficient) == 0 {
		c.Base.hot.Put(key, d, lt)
		c.Base.hotPuts.Inc()
	}
}
