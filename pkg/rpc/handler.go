package rpc

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gebn/ttlcache"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	handleDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "handle_duration_seconds",
			Help:      "Observes the time taken to handle peer requests, by cache.",
		},
		[]string{"cache"},
	)
	handleInFlight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "handle_in_flight",
			Help:      "The number of peer request responses in progress, by cache.",
		},
		[]string{"cache"},
	)
	handleResponses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "handle_responses_total",
			Help:      "The number of peer request responses sent, by cache and status code.",
		},
		[]string{"cache", "code"},
	)
)

// Handler returns a http.Handler to respond to requests sent by the PeerLoader
// returned by Loader(). We expect requests with a path beginning with prefix,
// which should match the first argument to http.Handle(), e.g. "/". Note it
// must end with a trailing slash in order to do a prefix rather than exact
// match.
func Handler(cache *ttlcache.Cache, prefix string, timeout time.Duration) http.Handler {
	return promhttp.InstrumentHandlerInFlight(
		handleInFlight.WithLabelValues(cache.Name),
		promhttp.InstrumentHandlerDuration(
			handleDuration.MustCurryWith(prometheus.Labels{
				"cache": cache.Name,
			}),
			promhttp.InstrumentHandlerCounter(
				handleResponses.MustCurryWith(prometheus.Labels{
					"cache": cache.Name,
				}),
				UninstrumentedHandler(cache, prefix, timeout),
			),
		),
	)
}

// determineCtx examines a request for a client-side timeout. If below the
// specified server-side timeout, it is used. If the client gave a bad value, we
// reject the request rather than fall back.
func determineCtx(req *http.Request, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if deadline := req.Header.Get(deadlineHeader); deadline != "" {
		seconds, err := strconv.ParseFloat(deadline, 64)
		if err != nil {
			return nil, nil, err
		}
		if duration := unixFractionalToDuration(seconds); duration < timeout {
			// only override if below our server-side maximum
			timeout = duration
		}
	}

	// no client-side deadline. Although a given node only sends peer requests
	// to the node it believes to be authoritative for a key, this node may have
	// a different view of the world, and fill it from its hot cache, or indeed
	// send it on to another node. Timeouts are essential here to ensure
	// requests don't circulate around the cluster forever.
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	return ctx, cancel, nil
}

// UninstrumentedHandler provides the underlying handler used by Handler,
// without instrumentation. This is intended to be a useful starting point for
// an external request handler.
func UninstrumentedHandler(cache *ttlcache.Cache, prefix string, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, prefix)
		if key == "" {
			http.Error(w, "key cannot be empty", http.StatusNotFound)
			return
		}

		ctx, cancel, err := determineCtx(r, timeout)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer cancel()

		d, lt, err := cache.Get(ctx, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		header := w.Header()
		// save Write() from trying to guess
		header.Set("Content-Type", "application/octet-stream")
		header.Set("Content-Length", strconv.Itoa(len(d)))
		header.Set("Expires", lt.Expires().Format(http.TimeFormat))
		setTimeHeader(header, lifetimeCreatedHeader, lt.Created)
		setDurationHeader(header, lifetimeTTLHeader, lt.TTL)
		w.Write(d) // ignore error; cannot update headers given we've already started the body
	})
}
