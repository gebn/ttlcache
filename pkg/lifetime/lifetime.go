package lifetime

import (
	"time"
)

const (
	// MaxDuration is the longest duration representable with time.Duration.
	// This can be used to create a TTL that is effectively infinite.
	MaxDuration time.Duration = 1<<63 - 1
)

var (
	// Zero is the canonical zero-duration lifetime. It should be returned
	// instead of nil when Lifetime is used as a value time. Note a
	// zero-duration lifetime can still be useful for indicating the creation
	// time of a value, which is why New doesn't return this value if the
	// duration is 0. Using this value in a comparison to check for a zero
	// duration is likely a bug.
	Zero = Lifetime{}
)

// Lifetime represents when a value was created, and the time from that instant
// that it expires. This should be used as an immutable value type.
//
// We represent TTLs as a time of creation, plus a duration (which is the
// literal TTL). This allows finding out when a value was created from its TTL,
// without having to embed it in the value. We require this at a library level
// for implementing TTL overrides.
type Lifetime struct {

	// Created is when the value was produced or retrieved.
	Created time.Time

	// TTL is the duration after Created for which the value is valid.
	TTL time.Duration
}

// New creates a new lifetime, starting now, lasting for the specified duration.
func New(duration time.Duration) Lifetime {
	return Lifetime{
		Created: time.Now(),
		TTL:     duration,
	}
}

// Remaining returns the time left before the lifetime expires.
func (l Lifetime) Remaining() time.Duration {
	return l.Created.Add(l.TTL).Sub(time.Now())
}

// Expired returns true if the lifetime has passed, or false otherwise.
func (l Lifetime) Expired() bool {
	return l.Remaining() < 0
}

// Cap limits the lifetime's TTL to a duration. If the duration is below the
// current TTL, the original lifetime is returned.
func (l Lifetime) Cap(duration time.Duration) Lifetime {
	if l.TTL < duration {
		return l
	}
	return Lifetime{
		Created: l.Created,
		TTL:     duration,
	}
}
