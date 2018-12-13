package server

import (
	"context"
	"testing"
	"time"
)

func TestRequestThrottle(t *testing.T) {
	rt, _ := NewRequestThrottle(RequestThrottleConfig{
		Capacity: 3,
		Rate:     2,
	})

	ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
	expected := []error{nil, nil, nil, ErrRequestThrottleTimeout}

	// Should time out by the 3rd request.
	for i := 0; i < 4; i++ {
		err := rt.Request(ctx)
		if err != expected[i] {
			t.Errorf("Unexpected error returned")
		}
	}

	// 3rd, 4th request will be rate limited, but should
	// complete before the context expires.
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	expected = []error{nil, nil, nil, nil}
	for i := 0; i < 4; i++ {
		err := rt.Request(ctx)
		if err != expected[i] {
			t.Errorf("Unexpected error returned")
		}
	}
}
