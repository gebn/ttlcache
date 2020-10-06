package rpc

import (
	"net/http"
	"time"

	"github.com/gebn/ttlcache"
)

// Pair is a wrapper around Loader and Handler that makes it impossible to have
// an inconsistent base path or request timeout. Fields should not be modified
// post-creation.
type Pair struct {

	// Prefix is the pattern under which the loader and handler will operate,
	// e.g. /ttlcache/caches/<name>/keys/. This is passed unmodified when
	// binding the handler, so must end with a /. The loader and handler are
	// deliberately not cache-aware to allow registering different handlers
	// against different caches.
	Prefix string

	// Timeout is the per-request timeout, used for each attempt in Loader, and
	// for the Cache.Get() call in the handler (further bounded by the request
	// context). The number of attempts Loader makes is dictated by the deadline
	// on the context passed to it at load time.
	Timeout time.Duration
}

// Loader builds a PeerLoader that will request keys from the named cache, using
// the provided HTTP client.
func (p *Pair) Loader(name string, client *http.Client) ttlcache.PeerLoader {
	return Loader(name, client, p.Prefix, p.Timeout)
}

// Handle builds a Handler to serve requests from the provided cache, then binds
// it to the provided mux at the correct path.
func (p *Pair) Handle(mux *http.ServeMux, cache *ttlcache.Cache) {
	// without the trailing slash, the mux will not give us requests for
	// subpaths, which is everything we care about
	mux.Handle(p.Prefix, Handler(cache, p.Prefix, p.Timeout))
}
