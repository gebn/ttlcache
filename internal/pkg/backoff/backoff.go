// Package backoff is a wrapper around cenkalti/backoff, adding OTel.
package backoff

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracer = otel.Tracer("github.com/gebn/ttlcache/internal/pkg/backoff")

	// Permanent wraps an error, indicating to the underlying library that the
	// request should not be retried and the underlying error should be
	// returned.
	Permanent = backoff.Permanent
)

// Backoff retries the provided operation with an exponential back-off until
// the context deadline is reached.
func Backoff(ctx context.Context, op func(context.Context) error) error {
	span := trace.SpanFromContext(ctx)
	attempt := 0
	return backoff.RetryNotify(
		func() error {
			ctx, span := tracer.Start(ctx, "attempt", trace.WithAttributes(
				attribute.Int("attempt.number", attempt),
			))
			defer span.End()

			err := op(ctx)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "attempt failed")
			}
			attempt++
			return err
		},
		backoff.WithContext(backoff.NewExponentialBackOff(), ctx),
		func(err error, delay time.Duration) {
			span.AddEvent("backing off failed attempt", trace.WithAttributes(
				attribute.String("error", err.Error()),
				attribute.String("delay", delay.String()),
				attribute.Int("attempt", attempt-1),
			))
		},
	)
}
