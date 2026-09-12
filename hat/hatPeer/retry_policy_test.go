package hatPeer_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatPeer"
)

var retryPolicyTestError = errors.New("retry policy test failure")

func TestRetryPolicyRetriesIdempotentCallsWithStableIdentity(t *testing.T) {
	var mu sync.Mutex
	var delays []time.Duration
	var events []hatPeer.RetryEvent
	policy, err := hatPeer.NewRetryPolicy(hatPeer.RetryPolicyOptions{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
		DisableJitter:  true,
		Retryable:      func(error) bool { return true },
		Sleep: func(_ context.Context, delay time.Duration) error {
			mu.Lock()
			delays = append(delays, delay)
			mu.Unlock()
			return nil
		},
		Observer: func(event hatPeer.RetryEvent) {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("NewRetryPolicy() error = %v", err)
	}

	request := hatPeer.RetryRequest{
		Operation:      hatPeer.RetryIdempotentMutation,
		IdempotencyKey: "write-42",
		FencingToken:   17,
	}
	var attempts []hatPeer.RetryAttempt
	err = policy.Execute(context.Background(), request, func(_ context.Context, attempt hatPeer.RetryAttempt) error {
		attempts = append(attempts, attempt)
		if attempt.Attempt < 3 {
			return retryPolicyTestError
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(attempts) != 3 {
		t.Fatalf("attempt count = %d, want 3", len(attempts))
	}
	for i, attempt := range attempts {
		if attempt.Attempt != uint32(i+1) || attempt.Operation != request.Operation || attempt.IdempotencyKey != request.IdempotencyKey || attempt.FencingToken != request.FencingToken {
			t.Fatalf("attempt[%d] = %+v, want stable metadata and attempt %d", i, attempt, i+1)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(delays) != 2 || delays[0] != time.Millisecond || delays[1] != time.Millisecond {
		t.Fatalf("delays = %v, want two 1ms delays", delays)
	}
	if len(events) != 2 || events[0].FailedAttempt != 1 || events[0].NextAttempt != 2 || events[1].FailedAttempt != 2 || events[1].NextAttempt != 3 {
		t.Fatalf("retry events = %+v, want failed attempts 1 and 2", events)
	}
}

func TestRetryPolicyNeverRetriesNonIdempotentMutations(t *testing.T) {
	policy, err := hatPeer.NewRetryPolicy(hatPeer.RetryPolicyOptions{
		MaxAttempts: 3,
		Retryable:   func(error) bool { return true },
		Sleep:       func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewRetryPolicy() error = %v", err)
	}
	var calls int
	err = policy.Execute(context.Background(), hatPeer.RetryRequest{Operation: hatPeer.RetryNonIdempotentMutation}, func(context.Context, hatPeer.RetryAttempt) error {
		calls++
		return retryPolicyTestError
	})
	if !errors.Is(err, retryPolicyTestError) {
		t.Fatalf("Execute() error = %v, want original error", err)
	}
	if calls != 1 {
		t.Fatalf("call count = %d, want 1", calls)
	}
}

func TestRetryPolicyRequiresIdentityForIdempotentMutations(t *testing.T) {
	policy := hatPeer.NewDefaultRetryPolicy()
	tests := []hatPeer.RetryRequest{
		{Operation: hatPeer.RetryIdempotentMutation, FencingToken: 1},
		{Operation: hatPeer.RetryIdempotentMutation, IdempotencyKey: "write-42"},
	}
	for _, request := range tests {
		calls := 0
		err := policy.Execute(context.Background(), request, func(context.Context, hatPeer.RetryAttempt) error {
			calls++
			return nil
		})
		if !errors.Is(err, hatPeer.ErrRetryPolicyRequestInvalid) {
			t.Fatalf("request %+v error = %v, want ErrRetryPolicyRequestInvalid", request, err)
		}
		if calls != 0 {
			t.Fatalf("request %+v call count = %d, want 0", request, calls)
		}
	}
}

func TestRetryPolicyCancellationStopsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy, err := hatPeer.NewRetryPolicy(hatPeer.RetryPolicyOptions{
		MaxAttempts:    3,
		InitialBackoff: time.Second,
		MaxBackoff:     time.Second,
		DisableJitter:  true,
		Retryable:      func(error) bool { return true },
		Sleep: func(ctx context.Context, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("NewRetryPolicy() error = %v", err)
	}
	var calls int
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	err = policy.Execute(ctx, hatPeer.RetryRequest{Operation: hatPeer.RetryRead}, func(context.Context, hatPeer.RetryAttempt) error {
		calls++
		return retryPolicyTestError
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("call count = %d, want 1", calls)
	}
}

func TestRetryPolicyReturnsFinalErrorAtAttemptBound(t *testing.T) {
	policy, err := hatPeer.NewRetryPolicy(hatPeer.RetryPolicyOptions{
		MaxAttempts: 2,
		Retryable:   func(error) bool { return true },
		Sleep:       func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewRetryPolicy() error = %v", err)
	}
	calls := 0
	err = policy.Execute(context.Background(), hatPeer.RetryRequest{Operation: hatPeer.RetryRead}, func(context.Context, hatPeer.RetryAttempt) error {
		calls++
		return retryPolicyTestError
	})
	if !errors.Is(err, retryPolicyTestError) {
		t.Fatalf("Execute() error = %v, want final error", err)
	}
	if calls != 2 {
		t.Fatalf("call count = %d, want 2", calls)
	}
}

func TestRetryPolicyUsesCappedExponentialBackoff(t *testing.T) {
	var delays []time.Duration
	policy, err := hatPeer.NewRetryPolicy(hatPeer.RetryPolicyOptions{
		MaxAttempts:    4,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     4 * time.Millisecond,
		DisableJitter:  true,
		Retryable:      func(error) bool { return true },
		Sleep: func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewRetryPolicy() error = %v", err)
	}
	calls := 0
	err = policy.Execute(context.Background(), hatPeer.RetryRequest{Operation: hatPeer.RetryRead}, func(context.Context, hatPeer.RetryAttempt) error {
		calls++
		if calls == 4 {
			return nil
		}
		return retryPolicyTestError
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond}
	if len(delays) != len(want) {
		t.Fatalf("delays = %v, want %v", delays, want)
	}
	for i := range want {
		if delays[i] != want[i] {
			t.Fatalf("delays = %v, want %v", delays, want)
		}
	}
}

func TestRetryPolicyRejectsInvalidOptionsAndInputs(t *testing.T) {
	tests := []hatPeer.RetryPolicyOptions{
		{MaxAttempts: -1},
		{InitialBackoff: -time.Nanosecond},
		{InitialBackoff: time.Second, MaxBackoff: time.Millisecond},
		{JitterFraction: -0.1},
		{JitterFraction: 1.1},
	}
	for _, options := range tests {
		if _, err := hatPeer.NewRetryPolicy(options); err == nil {
			t.Fatalf("NewRetryPolicy(%+v) succeeded, want error", options)
		}
	}
	policy := hatPeer.NewDefaultRetryPolicy()
	if err := policy.Execute(nil, hatPeer.RetryRequest{Operation: hatPeer.RetryRead}, func(context.Context, hatPeer.RetryAttempt) error { return nil }); !errors.Is(err, hatPeer.ErrRetryPolicyContextRequired) {
		t.Fatalf("nil context error = %v, want ErrRetryPolicyContextRequired", err)
	}
	if err := policy.Execute(context.Background(), hatPeer.RetryRequest{Operation: hatPeer.RetryRead}, nil); !errors.Is(err, hatPeer.ErrRetryPolicyCallRequired) {
		t.Fatalf("nil call error = %v, want ErrRetryPolicyCallRequired", err)
	}
	oversizedKey := hatPeer.RetryRequest{
		Operation:      hatPeer.RetryIdempotentMutation,
		IdempotencyKey: strings.Repeat("x", hatPeer.MaxRetryPolicyIdempotencyKeyBytes+1),
		FencingToken:   1,
	}
	if err := policy.Execute(context.Background(), oversizedKey, func(context.Context, hatPeer.RetryAttempt) error { return nil }); !errors.Is(err, hatPeer.ErrRetryPolicyIdempotencyKeyInvalid) {
		t.Fatalf("oversized key error = %v, want ErrRetryPolicyIdempotencyKeyInvalid", err)
	}
}

var retryPolicyBenchmarkSink uint32

func benchmarkRetryCall(_ context.Context, attempt hatPeer.RetryAttempt) error {
	retryPolicyBenchmarkSink = attempt.Attempt
	return nil
}

func BenchmarkRetryPolicyDirectCall(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := benchmarkRetryCall(context.Background(), hatPeer.RetryAttempt{Attempt: 1}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRetryPolicySingleSuccessfulCall(b *testing.B) {
	policy := hatPeer.NewDefaultRetryPolicy()
	request := hatPeer.RetryRequest{Operation: hatPeer.RetryRead}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := policy.Execute(context.Background(), request, benchmarkRetryCall); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRetryPolicyThreeAttempts(b *testing.B) {
	policy, err := hatPeer.NewRetryPolicy(hatPeer.RetryPolicyOptions{
		MaxAttempts:   3,
		DisableJitter: true,
		Retryable:     func(error) bool { return true },
		Sleep:         func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		b.Fatalf("NewRetryPolicy() error = %v", err)
	}
	request := hatPeer.RetryRequest{Operation: hatPeer.RetryRead}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		attempts := uint32(0)
		if err := policy.Execute(context.Background(), request, func(context.Context, hatPeer.RetryAttempt) error {
			attempts++
			if attempts < 3 {
				return retryPolicyTestError
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}
