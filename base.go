package ttlcache

import (
	"github.com/gebn/ttlcache/internal/pkg/lru"
	"github.com/gebn/ttlcache/internal/pkg/singleflight"
	"github.com/gebn/ttlcache/pkg/lifetime"

	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	authoritativeCapacity = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "authoritative_capacity",
			Help:      "The number of entries that can exist in the authoritative LRU cache before the oldest is evicted, by cache name.",
		},
		[]string{"cache"},
	)
	authoritativeGets = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "authoritative_gets_total",
			Help:      "The number of get operations to the authoritative LRU cache, by cache name.",
		},
		[]string{"cache"},
	)
	authoritativeMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "authoritative_misses_total",
			Help:      "The number of get operations to the authoritative LRU cache that did not yield a value, by cache name.",
		},
		[]string{"cache"},
	)
	authoritativePuts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "authoritative_puts_total",
			Help:      "The number of put operations to the authoritative LRU cache, including overwrites and new entries with the potential for an eviction, by cache name.",
		},
		[]string{"cache"},
	)
	// we don't evict unless a cache is at capacity, so evictions > 0 means
	// we've reached saturation
	authoritativeEvictionsPrematureDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "authoritative_evictions_premature_duration_seconds",
			Help:      "Observes the time remaining on entries prematurely evicted from an authoritative LRU cache, by cache name. This is generally undesirable.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 512
		},
		[]string{"cache"},
	)
	authoritativeEvictionsExpiredDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "authoritative_evictions_expired_duration_seconds",
			Help:      "Observes the duration ago entries evicted from an authoritative LRU cache expired, by cache name. This is intended behaviour.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 512
		},
		[]string{"cache"},
	)
	hotCapacity = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "hot_capacity",
			Help:      "The number of entries that can exist in the hot LRU cache before the oldest is evicted, by cache name.",
		},
		[]string{"cache"},
	)
	hotGets = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "hot_gets_total",
			Help:      "The number of get operations to the hot LRU cache, by cache name.",
		},
		[]string{"cache"},
	)
	hotMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "hot_misses_total",
			Help:      "The number of get operations to the hot LRU cache that did not yield a value, by cache name.",
		},
		[]string{"cache"},
	)
	hotPuts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "hot_puts_total",
			Help:      "The number of put operations to the hot LRU cache, including overwrites and new entries with the potential for an eviction, by cache name.",
		},
		[]string{"cache"},
	)
	hotEvictionsPrematureDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "hot_evictions_premature_duration_seconds",
			Help:      "Observes the time remaining on entries prematurely evicted from an hot LRU cache, by cache name. This is generally undesirable.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 512
		},
		[]string{"cache"},
	)
	hotEvictionsExpiredDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "hot_evictions_expired_duration_seconds",
			Help:      "Observes the duration ago entries evicted from an hot LRU cache expired, by cache name. This is intended behaviour.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 512
		},
		[]string{"cache"},
	)
	baseConfigures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "base_configures_total",
			Help:      "The number of times a given base has been combined with reloadable configuration to produce a useable cache.",
		},
		[]string{"cache"},
	)

	// some of these durations may be influenced by the ctx, but we assume this
	// is rare and well above the SLO threshold

	// to remove peer requests, subtract
	// ttlcache_rpc_handle_duration_seconds_count (for each instance, before
	// aggregation)
	getDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "get_duration_seconds",
			Help:      "Observes the time taken by gets to the cache. This implicitly includes those from peers.",
		},
		[]string{"cache"},
	)
	getDedupeAttempts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "get_dedupe_attempts_total",
			Help:      "The number of gets that could not be answered via LRU caches, and had to get a key lock.",
		},
		[]string{"cache"},
	)
	getDedupeFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "get_dedupe_failures_total",
			Help:      "The number of gets that did not reuse the result of an in-progress get.",
		},
		[]string{"cache"},
	)
	getFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "get_failures_total",
			Help:      "The number of gets that returned an error.",
		},
		[]string{"cache"},
	)
	peerLoadDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "peer_load_duration_seconds",
			Help:      "Observes the time taken by loads from peers. This implies we were not authoritative.",
		},
		[]string{"cache"},
	)
	peerLoadFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "peer_load_failures_total",
			Help:      "The number of peer load requests that returned an error.",
		},
		[]string{"cache"},
	)
	// find authoritative gets with ttlcache_origin_load_duration_seconds_count
	// - ttlcache_peer_load_failures_total
	originLoadDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "origin_load_duration_seconds",
			Help:      "Observes the time taken by loads from the origin. We were authoritative, or the peer load failed.",
		},
		[]string{"cache"},
	)
	originLoadFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "origin_load_failures_total",
			Help:      "The number of origin load requests that returned an error.",
		},
		[]string{"cache"},
	)
)

// PeerPicker knows which member of a cluster owns a given key. It is used to
// find the authoritative node for a given key so origin loads are co-ordinated
// to avoid a thundering herd.
type PeerPicker interface {

	// PickPeer returns the cluster node that is authoritiative for key. If the
	// current node is authoritative, the cluster contains no members, or we are
	// the only member, it returns nil.
	PickPeer(key string) *memberlist.Node
}

// BaseOpts contains parameters when creating a new base cache. All are
// required, as there is no standard configuration.
type BaseOpts struct {

	// Name is like a namespace for the cache. It allows multiple instances
	// within a single program. This may be passed to other peers during peer
	// loads, so must be consistent within a given cluster.
	Name string

	// PeerPicker will tell the cache who is authoritative for a given key, so
	// it knows where to attempt an origin load in the first instance.
	PeerPicker PeerPicker

	// AuthoritativeCacheCapacity is the max number of entries to allow in the
	// LRU map for keys we own. It is implemented as an LRU for safety, however
	// should be sized appropriately to hold everything. Note the consistent
	// hash can be some way off, so it is recommended to allow 1.5x the
	// allocation if keys were perfectly distributed. Keys that we were
	// previously authoritative for, but no longer due to membership changes,
	// are expected to expire over time due to TTLs.
	AuthoritativeCacheCapacity uint

	// HotCacheCapacity is the max number of entries to allow in the LRU map for
	// keys we do not own, but see frequent requests for. This is purely an
	// optimisation; keys expiring from this map is not the end of the world, as
	// they are likely only one hop to a peer away. However, being generous here
	// reduces the effects of cluster resizes, and makes us more resilient to
	// failure of an origin, as keys are stored in more places. This should be
	// large enough to avoid thrashing, but small enough that all instances do
	// not have to have unreasonable amounts of memory.
	HotCacheCapacity uint

	// ParallelRequests is the anticipated number of concurrent gets to the
	// cache. This is used for initial sizing of a data structure in an
	// internal duplicate call suppression package.
	ParallelRequests uint
}

// Base contains the non-reloadable parts of a cache that last for its entire
// lifetime, including peer selection and internal LRU caches. It must be
// .Configure()d with the reloadable parameters to be useful.
type Base struct {
	Name       string
	peerPicker PeerPicker

	// we have to keep these separate, as otherwise all nodes would converge on
	// a common set of popular keys, which would be very inefficient for the
	// cluster. A peer load is always preferable to an origin load.
	//
	// As peers change, the set keys for which we are authoritative can change
	// at any time, behind our back. Keys may also return to our ownership when
	// a failure is resolved. As we're using LRU caches, values that fall out of
	// use will eventually be evicted naturally; in the meantime, they are only
	// a suboptimal use of memory. It would be futile to try to manage this. We
	// chose to not manually evict expired values for a similar reason.
	//
	// It is debatable whether we want to keep the authoritative and hot caches
	// separate. On one hand, they contain the same thing, and is it such a bad
	// thing if we drop our own keys in favour of those requested more often?
	// The decision was that, in aggregate across the cluster, yes. Separation
	// allows easier tracking or saturation and evictions, and ensures we can
	// handle larger caches by not having every member converge towards storing
	// the same set of active keys.
	authoritative, hot *lru.Cache
	flight             *singleflight.Group

	// authoritative LRU cache metrics
	authoritativeGets, authoritativeMisses, authoritativePuts prometheus.Counter

	// hot LRU cache metrics
	hotGets, hotMisses, hotPuts prometheus.Counter

	baseConfigures prometheus.Counter

	getDuration, peerLoadDuration, originLoadDuration                                       prometheus.Observer
	getDedupeAttempts, getDedupeFailures, getFailures, peerLoadFailures, originLoadFailures prometheus.Counter
}

// NewBase creates a new primed but not yet usable cache. This allocates the
// underlying LRU caches. This function should only be called once for a given
// cache name.
func NewBase(opts *BaseOpts) *Base {
	// this is a lot, however doing it here means we only have to do it once
	authoritativeCapacity.WithLabelValues(opts.Name).Set(float64(opts.AuthoritativeCacheCapacity))
	hotCapacity.WithLabelValues(opts.Name).Set(float64(opts.HotCacheCapacity))
	return &Base{
		Name:       opts.Name,
		peerPicker: opts.PeerPicker,
		flight:     singleflight.NewGroup(opts.ParallelRequests),

		authoritative: lru.NewCache(
			opts.AuthoritativeCacheCapacity,
			onEviction(
				authoritativeEvictionsPrematureDuration.WithLabelValues(opts.Name),
				authoritativeEvictionsExpiredDuration.WithLabelValues(opts.Name),
			),
		),
		hot: lru.NewCache(
			opts.HotCacheCapacity,
			onEviction(
				hotEvictionsPrematureDuration.WithLabelValues(opts.Name),
				hotEvictionsExpiredDuration.WithLabelValues(opts.Name),
			),
		),

		authoritativeGets:   authoritativeGets.WithLabelValues(opts.Name),
		authoritativeMisses: authoritativeMisses.WithLabelValues(opts.Name),
		authoritativePuts:   authoritativePuts.WithLabelValues(opts.Name),

		hotGets:   hotGets.WithLabelValues(opts.Name),
		hotMisses: hotMisses.WithLabelValues(opts.Name),
		hotPuts:   hotPuts.WithLabelValues(opts.Name),

		baseConfigures: baseConfigures.WithLabelValues(opts.Name),

		getDuration:        getDuration.WithLabelValues(opts.Name),
		getDedupeAttempts:  getDedupeAttempts.WithLabelValues(opts.Name),
		getDedupeFailures:  getDedupeFailures.WithLabelValues(opts.Name),
		getFailures:        getFailures.WithLabelValues(opts.Name),
		peerLoadDuration:   peerLoadDuration.WithLabelValues(opts.Name),
		peerLoadFailures:   peerLoadFailures.WithLabelValues(opts.Name),
		originLoadDuration: originLoadDuration.WithLabelValues(opts.Name),
		originLoadFailures: originLoadFailures.WithLabelValues(opts.Name),
	}
}

// onEviction observes the amount of validity values had left, or time they have
// been expired for, when they are evicted from the cache. We are particularly
// keen to avoid evicting still-valid authoritative keys.
func onEviction(premature prometheus.Observer, expired prometheus.Observer) lru.EvictionFunc {
	return lru.EvictionFunc(func(lt lifetime.Lifetime) {
		remaining := lt.Remaining().Seconds()
		if lt.Expired() {
			// will be <0, so make positive to avoid breaking the histogram
			expired.Observe(-remaining)
		} else {
			premature.Observe(remaining)
		}
	})
}
