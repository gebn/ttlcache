package rpc

import (
	"context"
	"fmt"
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

// parseKey validates the request path, extracts the cache name and key name,
// checks the cache name matches the cache we are serving from, and returns the
// key name.
func parseKey(path, prefix, cache string) (string, error) {
	trimmed := strings.TrimPrefix(path, prefix)
	fragments := strings.SplitN(trimmed, "/", 5) // allow splitting off one too many fragments to catch the error
	if len(fragments) != 4 {
		return "", fmt.Errorf("expected path of form caches/<cache>/keys/<key>, got %v", trimmed)
	}
	if fragments[1] != cache {
		return "", fmt.Errorf("expecting requests for cache '%v' but got '%v'", cache, fragments[1])
	}
	return fragments[3], nil
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

// Handler returns a http.Handler to respond to requests sent by Loader. We
// expect requests with a path beginning with prefix, which should match the
// first argument to http.Handle(), e.g. "/".
func Handler(cache *ttlcache.Cache, prefix string, timeout time.Duration) http.Handler {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, err := parseKey(r.URL.Path, prefix, cache.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
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
		setTimeHeader(header, lifetimeCreatedHeader, lt.Created)
		setDurationHeader(header, lifetimeTTLHeader, lt.TTL)
		w.Write(d) // ignore error; cannot update headers given we've already started the body
	})
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
				handler,
			),
		),
	)
}
