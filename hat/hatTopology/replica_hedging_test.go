package hatTopology_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatTopology"
)

var (
	replicaHedgeTestFirstError  = errors.New("replica hedge first failure")
	replicaHedgeTestSecondError = errors.New("replica hedge second failure")
)

func replicaHedgeTestCandidates() []string {
	return []string{"node-a", "node-b", "node-c"}
}

func TestExecuteReplicaHedgedReturnsFirstSuccessAndCancelsLoser(t *testing.T) {
	candidates := replicaHedgeTestCandidates()[:2]
	slowStarted := make(chan struct{})
	slowCanceled := make(chan struct{})
	fastStarted := make(chan struct{})
	var eventsMu sync.Mutex
	var events []hatTopology.ReplicaHedgeEvent
	policy, err := hatTopology.NewReplicaHedgePolicy(hatTopology.ReplicaHedgePolicyOptions{
		MaxAttempts: 2,
		HedgeDelay:  10 * time.Millisecond,
		Observer: func(event hatTopology.ReplicaHedgeEvent) {
			eventsMu.Lock()
			events = append(events, event)
			eventsMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("NewReplicaHedgePolicy() error = %v", err)
	}
	value, selected, err := hatTopology.ExecuteReplicaHedged(context.Background(), policy, candidates, func(ctx context.Context, candidate string) (string, error) {
		if candidate == "node-a" {
			close(slowStarted)
			<-ctx.Done()
			close(slowCanceled)
			return "", ctx.Err()
		}
		close(fastStarted)
		return "fast", nil
	})
	if err != nil {
		t.Fatalf("ExecuteReplicaHedged() error = %v", err)
	}
	if value != "fast" || selected != 1 {
		t.Fatalf("result = (%q, %d), want fast from candidate index 1", value, selected)
	}
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow candidate was not started")
	}
	select {
	case <-fastStarted:
	case <-time.After(time.Second):
		t.Fatal("fast candidate was not started")
	}
	select {
	case <-slowCanceled:
	case <-time.After(time.Second):
		t.Fatal("slow candidate was not canceled")
	}
	eventsMu.Lock()
	defer eventsMu.Unlock()
	if len(events) != 1 || events[0].Attempt != 2 || events[0].CandidateIndex != 1 || events[0].Delay != 10*time.Millisecond {
		t.Fatalf("hedge events = %+v, want one index-1 attempt-2 event", events)
	}
}

func TestExecuteReplicaHedgedStartsNextAfterFailureWithoutWaiting(t *testing.T) {
	var events []hatTopology.ReplicaHedgeEvent
	policy, err := hatTopology.NewReplicaHedgePolicy(hatTopology.ReplicaHedgePolicyOptions{
		MaxAttempts: 2,
		HedgeDelay:  time.Hour,
		Observer: func(event hatTopology.ReplicaHedgeEvent) {
			events = append(events, event)
		},
	})
	if err != nil {
		t.Fatalf("NewReplicaHedgePolicy() error = %v", err)
	}
	value, selected, err := hatTopology.ExecuteReplicaHedged(context.Background(), policy, replicaHedgeTestCandidates()[:2], func(_ context.Context, candidate string) (string, error) {
		if candidate == "node-a" {
			return "", replicaHedgeTestFirstError
		}
		return "fallback", nil
	})
	if err != nil {
		t.Fatalf("ExecuteReplicaHedged() error = %v", err)
	}
	if value != "fallback" || selected != 1 {
		t.Fatalf("result = (%q, %d), want fallback from candidate index 1", value, selected)
	}
	if len(events) != 1 || events[0].Delay != 0 {
		t.Fatalf("hedge events = %+v, want immediate failover event", events)
	}
}

func TestExecuteReplicaHedgedBoundsAttemptsAndPreservesFailureOrder(t *testing.T) {
	policy, err := hatTopology.NewReplicaHedgePolicy(hatTopology.ReplicaHedgePolicyOptions{
		MaxAttempts: 2,
		HedgeDelay:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewReplicaHedgePolicy() error = %v", err)
	}
	calls := 0
	_, _, err = hatTopology.ExecuteReplicaHedged(context.Background(), policy, replicaHedgeTestCandidates(), func(_ context.Context, candidate string) (string, error) {
		calls++
		if candidate == "node-a" {
			return "", replicaHedgeTestFirstError
		}
		return "", replicaHedgeTestSecondError
	})
	if !errors.Is(err, hatTopology.ErrReplicaHedgeAllFailed) {
		t.Fatalf("error = %v, want ErrReplicaHedgeAllFailed", err)
	}
	if !errors.Is(err, replicaHedgeTestFirstError) || !errors.Is(err, replicaHedgeTestSecondError) {
		t.Fatalf("error = %v, want both attempt errors", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want bounded count 2", calls)
	}
	var hedgeErr *hatTopology.ReplicaHedgeError
	if !errors.As(err, &hedgeErr) {
		t.Fatalf("error = %T, want *ReplicaHedgeError", err)
	}
	if len(hedgeErr.Failures) != 2 || hedgeErr.Failures[0].CandidateIndex != 0 || hedgeErr.Failures[1].CandidateIndex != 1 {
		t.Fatalf("failures = %+v, want index 0 then index 1", hedgeErr.Failures)
	}
}

func TestExecuteReplicaHedgedHonorsCanceledContextBeforeStarting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	policy := hatTopology.NewDefaultReplicaHedgePolicy()
	calls := 0
	_, _, err := hatTopology.ExecuteReplicaHedged(ctx, policy, replicaHedgeTestCandidates(), func(context.Context, string) (string, error) {
		calls++
		return "", nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("calls = %d, want no calls after cancellation", calls)
	}
}

func TestNewReplicaHedgePolicyRejectsInvalidOptions(t *testing.T) {
	for _, options := range []hatTopology.ReplicaHedgePolicyOptions{
		{MaxAttempts: -1},
		{MaxAttempts: hatTopology.MaxReplicaHedgeAttempts + 1},
		{HedgeDelay: -time.Nanosecond},
	} {
		if _, err := hatTopology.NewReplicaHedgePolicy(options); err == nil {
			t.Fatalf("NewReplicaHedgePolicy(%+v) succeeded, want error", options)
		}
	}
	policy := hatTopology.NewDefaultReplicaHedgePolicy()
	if _, _, err := hatTopology.ExecuteReplicaHedged(nil, policy, replicaHedgeTestCandidates(), func(context.Context, string) (string, error) { return "", nil }); !errors.Is(err, hatTopology.ErrReplicaHedgeContextRequired) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, _, err := hatTopology.ExecuteReplicaHedged(context.Background(), policy, nil, func(context.Context, string) (string, error) { return "", nil }); !errors.Is(err, hatTopology.ErrReplicaHedgeCandidatesRequired) {
		t.Fatalf("empty candidates error = %v", err)
	}
}

func TestExecuteReplicaHedgedSingleCandidateReturnsWithoutFallback(t *testing.T) {
	policy, err := hatTopology.NewReplicaHedgePolicy(hatTopology.ReplicaHedgePolicyOptions{MaxAttempts: 1})
	if err != nil {
		t.Fatalf("NewReplicaHedgePolicy() error = %v", err)
	}
	candidate := replicaHedgeTestCandidates()[0]
	calls := 0
	value, selected, err := hatTopology.ExecuteReplicaHedged(context.Background(), policy, []string{candidate}, func(_ context.Context, got string) (string, error) {
		calls++
		if got != candidate {
			t.Fatalf("candidate = %q, want %q", got, candidate)
		}
		return "single", nil
	})
	if err != nil {
		t.Fatalf("ExecuteReplicaHedged() error = %v", err)
	}
	if value != "single" || selected != 0 || calls != 1 {
		t.Fatalf("result = (%q, %d), calls = %d, want single candidate once", value, selected, calls)
	}
}

func BenchmarkReplicaReadHedged(b *testing.B) {
	policy, err := hatTopology.NewReplicaHedgePolicy(hatTopology.ReplicaHedgePolicyOptions{
		MaxAttempts: 2,
		HedgeDelay:  time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := hatTopology.ExecuteReplicaHedged(ctx, policy, replicaHedgeBenchmarkCandidates, replicaHedgeBenchmarkRead); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReplicaReadHedgedHealthyFirst(b *testing.B) {
	policy, err := hatTopology.NewReplicaHedgePolicy(hatTopology.ReplicaHedgePolicyOptions{
		MaxAttempts: 2,
		HedgeDelay:  time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	candidates := replicaHedgeBenchmarkCandidates
	ctx := context.Background()
	call := func(_ context.Context, candidate string) (string, error) {
		if candidate != "node-a" {
			return "", replicaHedgeTestSecondError
		}
		return "ok", nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := hatTopology.ExecuteReplicaHedged(ctx, policy, candidates, call); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReplicaReadHedgedSingleCandidate(b *testing.B) {
	policy, err := hatTopology.NewReplicaHedgePolicy(hatTopology.ReplicaHedgePolicyOptions{MaxAttempts: 1})
	if err != nil {
		b.Fatal(err)
	}
	candidate := replicaHedgeBenchmarkCandidates[:1]
	ctx := context.Background()
	call := func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := hatTopology.ExecuteReplicaHedged(ctx, policy, candidate, call); err != nil {
			b.Fatal(err)
		}
	}
}
