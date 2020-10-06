package rpc

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/gebn/ttlcache"
	"github.com/gebn/ttlcache/pkg/lifetime"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	loadDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "load_duration_seconds",
			Help:      "Observes the time taken by peer load HTTP requests, by cache.",
		},
		[]string{"cache"},
	)
	loadInFlight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "load_in_flight",
			Help:      "The number of peer load requests in progress, by cache.",
		},
		[]string{"cache"},
	)
	loadResponses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "load_responses_total",
			Help:      "The number of load responses received, by cache and status code.",
		},
		[]string{"cache", "code"},
	)
)

// Loader returns a new PeerLoader for the specified cache name, using the
// provided client, prefixing paths with the provided string, and allowing the
// provided duration per request attempt. The prefix should begin with a /.
func Loader(name string, client *http.Client, prefix string, attempt time.Duration) ttlcache.PeerLoader {
	if client.Transport == nil {
		// as we override the transport, the http package no longer knows to
		// swap in DefaultTransport - this avoids a panic
		client.Transport = http.DefaultTransport
	}
	client.Transport = promhttp.InstrumentRoundTripperInFlight(
		loadInFlight.WithLabelValues(name),
		promhttp.InstrumentRoundTripperDuration(
			loadDuration.MustCurryWith(prometheus.Labels{
				"cache": name,
			}),
			promhttp.InstrumentRoundTripperCounter(
				loadResponses.MustCurryWith(prometheus.Labels{
					"cache": name,
				}),
				client.Transport,
			),
		),
	)
	escapedName := url.PathEscape(name) // do once
	return ttlcache.PeerLoaderFunc(func(ctx context.Context, node *memberlist.Node, cache *ttlcache.Cache, key string) ([]byte, lifetime.Lifetime, error) {
		metadata := &Metadata{}
		if err := metadata.Decode(node.Meta); err != nil {
			return nil, lifetime.Zero, err
		}
		reqURL := &url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(node.Addr.String(), strconv.Itoa(int(metadata.Port))),
			Path:   path.Join(prefix, "caches", escapedName, "keys", url.PathEscape(key)), // check works with a key with a / in it
		}
		req, err := http.NewRequest(http.MethodGet, reqURL.String(), nil)
		if err != nil {
			return nil, lifetime.Zero, err
		}
		if deadline, ok := ctx.Deadline(); ok {
			req.Header.Add(deadlineHeader, fmt.Sprintf("%f", timeToUnixFractional(deadline)))
		}
		var resp *http.Response
		err = backoff.Retry(func() error {
			ctx, cancel := context.WithTimeout(ctx, attempt)
			attemptResp, err := client.Do(req.WithContext(ctx))
			cancel()
			if err != nil {
				return err
			}
			// must close body from now on
			if attemptResp.StatusCode != http.StatusOK {
				attemptResp.Body.Close()
				return fmt.Errorf("got HTTP %v", attemptResp.StatusCode)
			}
			// success, body will be closed in outer scope
			resp = attemptResp
			return nil
		}, backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
		if err != nil {
			// body has been closed
			return nil, lifetime.Zero, err
		}
		defer resp.Body.Close()
		value, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, lifetime.Zero, err
		}
		lt, err := parseLifetime(resp.Header)
		if err != nil {
			return nil, lifetime.Zero, err
		}
		return value, *lt, nil
	})
}

// parseLifetime extracts a lifetime object from response headers.
func parseLifetime(header http.Header) (*lifetime.Lifetime, error) {
	created, err := strconv.ParseFloat(header.Get(lifetimeCreatedHeader), 64)
	if err != nil {
		return nil, err
	}
	duration, err := strconv.ParseFloat(header.Get(lifetimeTTLHeader), 64)
	if err != nil {
		return nil, err
	}
	return &lifetime.Lifetime{
		Created: unixFractionalToTime(created),
		TTL:     unixFractionalToDuration(duration),
	}, nil
}
