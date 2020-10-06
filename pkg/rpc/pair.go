package rpc

import (
	"net/http"
	"time"

	"github.com/gebn/ttlcache"
)

// Pair is a wrapper around Loader and Handler that makes it impossible to have
// an inconsistent base path or request timeout. Fields are exposed, but should
// not be modified post-creation.
type Pair struct {

	// BasePath is the prefix under which the loader and handler will operate,
	// e.g. /ttlcache/caches/<name>. The loader and handler are deliberately not
	// cache-aware to allow registering different handlers against different
	// caches.
	BasePath string

	// Timeout is the per-request timeout, used for each attempt in Loader, and
	// for the Cache.Get() call in the handler (further bounded by the request
	// context). The number of attempts Loader makes is dictated by the context
	// passed to it at load time.
	Timeout time.Duration
}

// Loader builds a PeerLoader that will request keys from the named cache, using
// the provided HTTP client.
func (p *Pair) Loader(name string, client *http.Client) ttlcache.PeerLoader {
	return Loader(name, client, p.BasePath, p.Timeout)
}

// Handle builds a Handler to serve requests from the provided cache, then binds
// it to the provided mux at the correct path.
func (p *Pair) Handle(mux *http.ServeMux, cache *ttlcache.Cache) {
	// without the trailing slash, the mux will not give us requests for
	// subpaths, which is everything we care about
	path := p.BasePath + "/"
	mux.Handle(path, Handler(cache, path, p.Timeout))
}
