package hatReplication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecuteWriteQuorumUntilSatisfiedReturnsAfterRequiredAcknowledgements(t *testing.T) {
	slowStarted := make(chan struct{})
	slowCanceled := make(chan struct{})

	result, err := ExecuteWriteQuorumUntilSatisfied(context.Background(), []string{"fast-a", "fast-b", "slow"}, 2, func(ctx context.Context, node string) error {
		if node != "slow" {
			<-slowStarted
			return nil
		}
		close(slowStarted)
		<-ctx.Done()
		close(slowCanceled)
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("ExecuteWriteQuorumUntilSatisfied() error = %v, want nil", err)
	}
	if !result.Decision.Satisfied || result.Decision.Acknowledged != 2 {
		t.Fatalf("decision = %#v, want two acknowledged writes", result.Decision)
	}
	if result.Pending != 1 {
		t.Fatalf("pending = %d, want one canceled slow write", result.Pending)
	}
	if len(result.Attempts) != 2 || result.Attempts[0].Node != "fast-a" || result.Attempts[1].Node != "fast-b" {
		t.Fatalf("completed attempts = %#v, want the two fast targets in input order", result.Attempts)
	}
	select {
	case <-slowCanceled:
	case <-time.After(time.Second):
		t.Fatal("slow target did not observe cancellation")
	}
}

func TestExecuteWriteQuorumUntilSatisfiedStopsWhenQuorumIsImpossible(t *testing.T) {
	slowCanceled := make(chan struct{})
	allStarted := make(chan struct{})
	fastFinished := make(chan struct{})
	var started atomic.Int32

	result, err := ExecuteWriteQuorumUntilSatisfied(context.Background(), []string{"fast", "failed", "slow"}, 3, func(ctx context.Context, node string) error {
		if started.Add(1) == 3 {
			close(allStarted)
		}
		select {
		case <-allStarted:
		case <-ctx.Done():
			if node == "slow" {
				close(slowCanceled)
			}
			return ctx.Err()
		}
		switch node {
		case "fast":
			close(fastFinished)
			return nil
		case "failed":
			<-fastFinished
			return errors.New("replica unavailable")
		default:
			<-ctx.Done()
			close(slowCanceled)
			return ctx.Err()
		}
	})
	if !errors.Is(err, ErrWriteQuorumUnsatisfied) {
		t.Fatalf("error = %v, want ErrWriteQuorumUnsatisfied", err)
	}
	if result.Decision.Satisfied || result.Decision.Acknowledged != 1 {
		t.Fatalf("decision = %#v, want one acknowledgement and an unsatisfied quorum", result.Decision)
	}
	select {
	case <-slowCanceled:
	case <-time.After(time.Second):
		t.Fatal("slow target did not observe cancellation after quorum became impossible")
	}
}

func TestExecuteWriteQuorumUntilSatisfiedWaitsForAllWhenRequiredEqualsTotal(t *testing.T) {
	result, err := ExecuteWriteQuorumUntilSatisfied(context.Background(), []string{"a", "b", "c"}, 3, func(context.Context, string) error {
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteWriteQuorumUntilSatisfied() error = %v, want nil", err)
	}
	if result.Pending != 0 || len(result.Attempts) != 3 {
		t.Fatalf("result = %#v, want all targets completed", result)
	}
}

func TestExecuteWriteQuorumUntilSatisfiedRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := ExecuteWriteQuorumUntilSatisfied(ctx, []string{"a"}, 1, func(context.Context, string) error {
		t.Fatal("canceled quorum invoked write callback")
		return nil
	})
	if !errors.Is(err, ErrWriteQuorumContextCanceled) {
		t.Fatalf("error = %v, want ErrWriteQuorumContextCanceled", err)
	}
	if result.Pending != 0 || len(result.Attempts) != 1 || result.Attempts[0].Error == "" {
		t.Fatalf("result = %#v, want one canceled attempt", result)
	}
}
