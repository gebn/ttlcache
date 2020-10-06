package rpc

import (
	"net/http"
	"time"

	"github.com/gebn/ttlcache"
)

// Pair is a wrapper around Loader and Handler that makes it impossible to have
// an inconsistent base path.
type Pair struct {
	basePath string
}

// NewPair returns a new Pair wrapper that will use the specified path, e.g.
// /ttlcache.
func NewPair(basePath string) *Pair {
	return &Pair{
		basePath: basePath,
	}
}

func (p *Pair) Loader(name string, client *http.Client, timeout time.Duration) ttlcache.PeerLoader {
	return Loader(name, client, p.basePath, timeout)
}

func (p *Pair) Handle(mux *http.ServeMux, cache *ttlcache.Cache) {
	// without the trailing slash, the mux will not give us requests for
	// subpaths, which is everything we care about
	path := p.basePath + "/"
	mux.Handle(path, Handler(cache, path))
}
