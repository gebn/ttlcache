package rpc

import (
	"net/http"
	"time"

	"github.com/gebn/ttlcache"
)

// Pair is a wrapper around Loader and Handler that makes it impossible to have
// an inconsistent base path. Fields should not be modified post-creation.
type Pair struct {

	// Prefix is the pattern under which the loader and handler will operate,
	// e.g. /ttlcache/caches/<name>/keys/. This is passed unmodified when
	// binding the handler, so must end with a /. The loader and handler are
	// deliberately not cache-aware to allow registering different handlers
	// against different caches.
	Prefix string
}

// Loader builds a PeerLoader that will request keys from the named cache, using
// the provided HTTP client. The Timeout of the client controls the time to
// allow for each request.
func (p *Pair) Loader(name string, client *http.Client) ttlcache.PeerLoader {
	return Loader(name, client, p.Prefix)
}

// Handle builds a Handler to serve requests from the provided cache, then
// binds it to the provided mux at the correct path. The timeout is a safety
// mechanism to ensure a request doesn't bounce around the cluster forever,
// even if a client is willing to wait.
func (p *Pair) Handle(mux *http.ServeMux, cache *ttlcache.Cache, timeout time.Duration) {
	// without the trailing slash, the mux will not give us requests for
	// subpaths, which is everything we care about
	mux.Handle(p.Prefix, Handler(cache, p.Prefix, timeout))
}
