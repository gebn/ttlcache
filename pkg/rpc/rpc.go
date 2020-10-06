// Package rpc implements a reference PeerLoader and corresponding handler.
package rpc

import (
	"fmt"
	"net/http"
	"time"
)

const (
	namespace = "ttlcache"
	subsystem = "rpc"

	// deadlineHeader is the name of the HTTP request header indicating when the
	// client is willing to wait until, specified as the float number of seconds
	// since the Unix epoch.
	deadlineHeader = "X-Deadline"

	// lifetimeCreatedHeader is the name of the HTTP response header indicating when
	// the value was last retrieved from the origin, specified as the float
	// number of seconds since the Unix epoch.
	lifetimeCreatedHeader = "X-Lifetime-Created"

	// lifetimeTTLHeader is the name of the HTTP response header indicating the
	// TTL of the value from the point of creation, specified as a float number
	// of seconds.
	lifetimeTTLHeader = "X-Lifetime-Duration"
)

func timeToUnixFractional(t time.Time) float64 {
	return float64(t.UnixNano()) / float64(time.Second/time.Nanosecond)
}

func unixFractionalToTime(t float64) time.Time {
	return time.Unix(0, int64(t*float64(time.Second/time.Nanosecond)))
}

func durationToUnixFractional(d time.Duration) float64 {
	return float64(d) / float64(time.Second/time.Nanosecond)
}

func unixFractionalToDuration(d float64) time.Duration {
	return time.Duration(d * float64(time.Second/time.Nanosecond))
}

func setTimeHeader(header http.Header, name string, value time.Time) {
	header.Set(name, fmt.Sprintf("%f", timeToUnixFractional(value)))
}

func setDurationHeader(header http.Header, name string, value time.Duration) {
	header.Set(name, fmt.Sprintf("%f", durationToUnixFractional(value)))
}
