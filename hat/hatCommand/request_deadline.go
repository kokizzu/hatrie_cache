package hatCommand

import (
	"context"
	"errors"
	"time"
)

// ErrInvalidRequestTimeout reports a negative request timeout.
var ErrInvalidRequestTimeout = errors.New("request timeout must not be negative")

// WithRequestTimeout applies an optional maximum duration to ctx. A zero
// timeout preserves ctx and does not allocate. An earlier parent deadline is
// preserved instead of wrapping it in another timer.
func WithRequestTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout < 0 {
		return ctx, noopCancel, ErrInvalidRequestTimeout
	}
	if timeout == 0 {
		return ctx, noopCancel, nil
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= timeout {
		return ctx, noopCancel, nil
	}
	derived, cancel := context.WithTimeout(ctx, timeout)
	return derived, cancel, nil
}

func noopCancel() {}
