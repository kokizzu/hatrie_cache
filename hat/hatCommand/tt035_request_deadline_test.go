package hatCommand

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTT035WithRequestTimeoutDisabledPreservesContext(t *testing.T) {
	parent, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	got, cancel, err := WithRequestTimeout(parent, 0)
	if err != nil {
		t.Fatalf("WithRequestTimeout(disabled) error = %v", err)
	}
	if got != parent {
		t.Fatalf("WithRequestTimeout(disabled) returned a different context")
	}
	cancel()
	if got.Err() != nil {
		t.Fatalf("disabled timeout cancel changed parent context: %v", got.Err())
	}
}

func TestTT035WithRequestTimeoutRejectsNegativeDuration(t *testing.T) {
	got, cancel, err := WithRequestTimeout(context.Background(), -time.Nanosecond)
	cancel()
	if !errors.Is(err, ErrInvalidRequestTimeout) {
		t.Fatalf("WithRequestTimeout(negative) error = %v, want %v", err, ErrInvalidRequestTimeout)
	}
	if got == nil {
		t.Fatal("WithRequestTimeout(negative) returned nil context")
	}
}

func TestTT035WithRequestTimeoutPreservesEarlierParentDeadline(t *testing.T) {
	parent, parentCancel := context.WithTimeout(context.Background(), time.Hour)
	defer parentCancel()

	got, cancel, err := WithRequestTimeout(parent, 2*time.Hour)
	if err != nil {
		t.Fatalf("WithRequestTimeout(later) error = %v", err)
	}
	defer cancel()
	if got != parent {
		t.Fatalf("WithRequestTimeout(later) replaced an earlier parent deadline")
	}
}

func TestTT035WithRequestTimeoutExpiresDerivedContext(t *testing.T) {
	got, cancel, err := WithRequestTimeout(context.Background(), 5*time.Millisecond)
	if err != nil {
		t.Fatalf("WithRequestTimeout(active) error = %v", err)
	}
	defer cancel()
	if _, ok := got.Deadline(); !ok {
		t.Fatal("WithRequestTimeout(active) did not install a deadline")
	}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-got.Done():
	case <-timer.C:
		t.Fatal("WithRequestTimeout(active) did not expire")
	}
	if !errors.Is(got.Err(), context.DeadlineExceeded) {
		t.Fatalf("derived context error = %v, want deadline exceeded", got.Err())
	}
}

func BenchmarkTT035WithRequestTimeoutDisabled(b *testing.B) {
	parent := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, cancel, err := WithRequestTimeout(parent, 0)
		if err != nil || got != parent {
			b.Fatalf("disabled request timeout = (%v, %v, %v)", got, cancel, err)
		}
		cancel()
	}
}

func BenchmarkTT035WithRequestTimeoutEnabled(b *testing.B) {
	parent := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, cancel, err := WithRequestTimeout(parent, time.Hour)
		if err != nil {
			b.Fatal(err)
		}
		cancel()
	}
}
