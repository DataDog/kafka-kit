package server

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrRequestThrottleTimeout error.
	ErrRequestThrottleTimeout = errors.New("wait time exceeded")
)

// RequestThrottle controls request rates with
// a configurable burst capacity and per-second rate
// backed with a token bucket.
type RequestThrottle interface {
	Request(context.Context) error
}

type requestThrottle struct {
	c chan struct{}
}

// RequestThrottleConfig specifies the RequestThrottle
// burst capacity and per-second rate limit.
type RequestThrottleConfig struct {
	// Burst capacity.
	Capacity int
	// Request rate (reqs/s).
	Rate int
}

// NewRequestThrottle initializes a RequestThrottle.
func NewRequestThrottle(cfg RequestThrottleConfig) (RequestThrottle, error) {
	switch {
	case cfg.Rate < 1:
		return nil, errors.New("rate must be >= 1")
	case cfg.Capacity < 1:
		return nil, errors.New("capacity must be >= 1")
	}

	t := &requestThrottle{
		c: make(chan struct{}, cfg.Capacity),
	}

	// Background refill.
	d := time.Duration(1000000000 / cfg.Rate)
	refill := time.NewTicker(d * time.Nanosecond)
	go func() {
		for range refill.C {
			<-t.c
		}
	}()

	return t, nil
}

// Request takes a context and attempts to acquire a token
// from the requestThrottle. An error is returned if a free
// token is not acquired by the context expiration.
func (t *requestThrottle) Request(ctx context.Context) error {
	select {
	case t.c <- struct{}{}:
		break
	case <-ctx.Done():
		return ErrRequestThrottleTimeout
	}

	return nil
}
