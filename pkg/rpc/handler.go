package rpc

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

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

// Handler returns a http.Handler to respond to requests sent by Loader. We
// expect requests with a path beginning with prefix, which should match the
// first argument to http.Handle(), e.g. "/".
func Handler(cache *ttlcache.Cache, prefix string) http.Handler {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, prefix)
		fragments := strings.SplitN(path, "/", 5) // allow splitting off one too many fragments to catch the error
		if len(fragments) != 4 {
			http.Error(w, fmt.Sprintf("expected path of form caches/<cache>/keys/<key>, got %v", path), http.StatusNotFound)
			return
		}
		if fragments[1] != cache.Name {
			http.Error(w, fmt.Sprintf("expecting requests for cache '%v' but got '%v'", cache.Name, fragments[1]), http.StatusInternalServerError)
			return
		}
		ctx := r.Context() // no timeout if the client did not ask for one
		if deadlineStr := r.Header.Get(deadlineHeader); deadlineStr != "" {
			deadline, err := strconv.ParseFloat(deadlineStr, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, unixFractionalToDuration(deadline))
			defer cancel()
		}
		// although a given node only sends peer requests to the node it
		// believes to be authoritative for a key, this node may have a
		// different view of the world, and fill it from its hot cache, or
		// indeed send it on to another node. Timeouts are essential here.
		d, lt, err := cache.Get(ctx, fragments[3])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		headers := w.Header()
		headers.Add("Content-Type", "application/octet-stream")
		headers.Add("Content-Length", strconv.Itoa(len(d)))
		headers.Add(lifetimeCreatedHeader, fmt.Sprintf("%f", timeToUnixFractional(lt.Created)))
		headers.Add(lifetimeTTLHeader, fmt.Sprintf("%f", durationToUnixFractional(lt.TTL)))
		if _, err := w.Write(d); err != nil {
			// unlikely to succeed given we've already started the body
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
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
