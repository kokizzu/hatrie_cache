package hatPeer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectionPoolAdaptiveCircuitBreakerBacksOffAndDecays(t *testing.T) {
	const baseInterval = time.Millisecond
	var dialed atomic.Int32
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxDialAttempts: 1,
		Breaker: &ConnectionPoolCircuitBreakerOptions{
			FailureThreshold: 1,
			OpenInterval:     baseInterval,
			Adaptive: &ConnectionPoolCircuitBreakerAdaptiveOptions{
				MaxOpenInterval: 4 * baseInterval,
				BackoffFactor:   2,
			},
		},
		Dial: func(context.Context) (Connection, error) {
			if dialed.Add(1) == 3 {
				return &connectionPoolTestConnection{}, nil
			}
			return nil, errors.New("peer unavailable")
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	defer func() {
		if err := p.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); err == nil {
		t.Fatal("first Do() unexpectedly succeeded")
	}
	stats := p.CircuitBreakerStats()
	if !stats.Adaptive || stats.State != ConnectionPoolCircuitOpen || stats.Opens != 1 || stats.CurrentOpenInterval != baseInterval {
		t.Fatalf("first breaker stats = %#v, want adaptive one-interval open", stats)
	}

	if err := waitForAdaptiveDial(t, p, &dialed, 2); err == nil {
		t.Fatal("second Do() unexpectedly succeeded")
	}
	stats = p.CircuitBreakerStats()
	if stats.State != ConnectionPoolCircuitOpen || stats.Opens != 2 || stats.CurrentOpenInterval != 2*baseInterval {
		t.Fatalf("backed-off breaker stats = %#v, want two-interval open", stats)
	}

	if err := waitForAdaptiveDial(t, p, &dialed, 3); err != nil {
		t.Fatalf("recovery Do() error = %v", err)
	}
	stats = p.CircuitBreakerStats()
	if stats.State != ConnectionPoolCircuitClosed || stats.ConsecutiveFailures != 0 || stats.Probes != 2 || stats.CurrentOpenInterval != baseInterval {
		t.Fatalf("recovered breaker stats = %#v, want closed at base interval", stats)
	}
}

func waitForAdaptiveDial(t *testing.T, p *ConnectionPool, dialed *atomic.Int32, want int32) error {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var lastErr error
	for dialed.Load() < want {
		lastErr = p.Do(context.Background(), func(context.Context, Connection) error { return nil })
		if lastErr != nil && !errors.Is(lastErr, ErrConnectionPoolCircuitOpen) {
			t.Fatalf("Do() while waiting for dial %d = %v", want, lastErr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial attempt %d did not occur; last error = %v", want, lastErr)
		}
		time.Sleep(100 * time.Microsecond)
	}
	return lastErr
}

func TestConnectionPoolAdaptiveCircuitBreakerValidatesOptions(t *testing.T) {
	tests := []struct {
		name    string
		breaker *ConnectionPoolCircuitBreakerOptions
		wantErr error
	}{
		{
			name: "maximum below base",
			breaker: &ConnectionPoolCircuitBreakerOptions{
				OpenInterval: 2 * time.Second,
				Adaptive:     &ConnectionPoolCircuitBreakerAdaptiveOptions{MaxOpenInterval: time.Second},
			},
			wantErr: ErrConnectionPoolCircuitAdaptiveMaxInvalid,
		},
		{
			name: "negative backoff",
			breaker: &ConnectionPoolCircuitBreakerOptions{
				Adaptive: &ConnectionPoolCircuitBreakerAdaptiveOptions{BackoffFactor: -1},
			},
			wantErr: ErrConnectionPoolCircuitAdaptiveBackoffInvalid,
		},
		{
			name: "backoff too large",
			breaker: &ConnectionPoolCircuitBreakerOptions{
				Adaptive: &ConnectionPoolCircuitBreakerAdaptiveOptions{BackoffFactor: MaxConnectionPoolCircuitAdaptiveBackoffFactor + 1},
			},
			wantErr: ErrConnectionPoolCircuitAdaptiveBackoffInvalid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConnectionPool(ConnectionPoolOptions{
				Breaker: tt.breaker,
				Dial:    func(context.Context) (Connection, error) { return nil, errors.New("unused") },
			})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewConnectionPool() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestIncreaseCircuitOpenIntervalCaps(t *testing.T) {
	tests := []struct {
		name    string
		current time.Duration
		maximum time.Duration
		factor  int
		want    time.Duration
	}{
		{name: "doubles", current: time.Second, maximum: 4 * time.Second, factor: 2, want: 2 * time.Second},
		{name: "caps multiplication", current: 3 * time.Second, maximum: 4 * time.Second, factor: 2, want: 4 * time.Second},
		{name: "already capped", current: 4 * time.Second, maximum: 4 * time.Second, factor: 2, want: 4 * time.Second},
		{name: "factor one", current: time.Second, maximum: 4 * time.Second, factor: 1, want: time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := increaseCircuitOpenInterval(tt.current, tt.maximum, tt.factor); got != tt.want {
				t.Fatalf("increaseCircuitOpenInterval() = %s, want %s", got, tt.want)
			}
		})
	}
}

func BenchmarkConnectionPoolDialStormWithAdaptiveBreaker(b *testing.B) {
	wantErr := errors.New("peer unavailable")
	var dialed atomic.Int64
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxDialAttempts: 1,
		Breaker: &ConnectionPoolCircuitBreakerOptions{
			FailureThreshold: 1,
			OpenInterval:     time.Hour,
			Adaptive: &ConnectionPoolCircuitBreakerAdaptiveOptions{
				MaxOpenInterval: 24 * time.Hour,
			},
		},
		Dial: func(context.Context) (Connection, error) {
			dialed.Add(1)
			return nil, wantErr
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	_ = p.Do(context.Background(), func(context.Context, Connection) error { return nil })
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = p.Do(context.Background(), func(context.Context, Connection) error { return nil })
	}
	b.StopTimer()
	b.ReportMetric(float64(dialed.Load()), "dial-calls")
	if err := p.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}
