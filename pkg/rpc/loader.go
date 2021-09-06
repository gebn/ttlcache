package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/gebn/ttlcache"
	"github.com/gebn/ttlcache/internal/pkg/backoff"
	"github.com/gebn/ttlcache/pkg/lifetime"

	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
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

// Loader returns a new PeerLoader that will send requests to
// <addr>:<metaport><basePath>/keys/<key> using the provided client. As we
// modify the client's transport, it should not be used for any other purpose,
// as the requests will lead to incorrect metrics. The timeout of each request
// is controlled by the client's Timeout. The loader implementation will back-off
// exponentially until the context expires. If non-empty, the base path must
// begin with a /.
func Loader(cache string, client *http.Client, basePath string) ttlcache.PeerLoader {
	// we demand the cache name only so we can initialise time series
	// immediately without waiting for the first outgoing peer request
	if client.Transport == nil {
		// as we override the transport, the http package no longer knows to
		// swap in DefaultTransport - this avoids a panic
		client.Transport = http.DefaultTransport
	}
	client.Transport = promhttp.InstrumentRoundTripperInFlight(
		loadInFlight.WithLabelValues(cache),
		promhttp.InstrumentRoundTripperDuration(
			loadDuration.MustCurryWith(prometheus.Labels{
				"cache": cache,
			}),
			promhttp.InstrumentRoundTripperCounter(
				loadResponses.MustCurryWith(prometheus.Labels{
					"cache": cache,
				}),
				otelhttp.NewTransport(client.Transport),
			),
		),
	)
	return ttlcache.PeerLoaderFunc(func(ctx context.Context, node *memberlist.Node, _ *ttlcache.Cache, key string) ([]byte, lifetime.Lifetime, error) {
		req, err := buildReq(node, basePath, key)
		if err != nil {
			return nil, lifetime.Zero, err
		}
		var resp *http.Response
		err = backoff.Backoff(ctx, func(ctx context.Context) error {
			// we wouldn't need this were it not for our desire to indicate to
			// the server what our timeout is
			ctx, cancel := context.WithTimeout(ctx, client.Timeout)
			defer cancel()
			if deadline, ok := ctx.Deadline(); ok { // always expect to be true
				// must replace any existing header from previous attempt
				setTimeHeader(req.Header, deadlineHeader, deadline)
			}
			resp, err = doAttempt(client, req.WithContext(ctx))
			return err
		})
		if err != nil {
			// body has been closed
			return nil, lifetime.Zero, err
		}
		defer resp.Body.Close()
		value, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, lifetime.Zero, err
		}
		lt, err := parseLifetime(resp.Header)
		if err != nil {
			return nil, lifetime.Zero, err
		}
		if len(value) == 0 {
			// we are required to return nil rather than a zero-length []byte
			return nil, lt, nil
		}
		return value, lt, nil
	})
}

func buildHost(node *memberlist.Node) (string, error) {
	metadata := &Metadata{}
	if err := metadata.Decode(node.Meta); err != nil {
		return "", err
	}
	return net.JoinHostPort(node.Addr.String(), strconv.FormatUint(uint64(metadata.Port), 10)), nil
}

func buildReq(node *memberlist.Node, basePath, key string) (*http.Request, error) {
	host, err := buildHost(node)
	if err != nil {
		return nil, err
	}
	reqURL := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   path.Join(basePath, "keys", url.PathEscape(key)), // TODO check works with a key with a / in it
	}
	return http.NewRequest(http.MethodGet, reqURL.String(), nil)
}

// attempt closes the body on error, leaving it open otherwise.
func doAttempt(client *http.Client, r *http.Request) (*http.Response, error) {
	resp, err := client.Do(r)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("got HTTP %v", resp.StatusCode)
	}
	return resp, nil
}

// parseLifetime extracts a lifetime object from response headers.
func parseLifetime(header http.Header) (lifetime.Lifetime, error) {
	created, err := strconv.ParseFloat(header.Get(lifetimeCreatedHeader), 64)
	if err != nil {
		return lifetime.Zero, err
	}
	duration, err := strconv.ParseFloat(header.Get(lifetimeTTLHeader), 64)
	if err != nil {
		return lifetime.Zero, err
	}
	return lifetime.Lifetime{
		Created: unixFractionalToTime(created),
		TTL:     unixFractionalToDuration(duration),
	}, nil
}
