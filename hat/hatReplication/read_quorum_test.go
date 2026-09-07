package hatReplication_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestExecuteReadQuorumReturnsDeterministicMatchingValue(t *testing.T) {
	var calls atomic.Int32
	result, err := hatReplication.ExecuteReadQuorum(context.Background(), []string{"east", "west", "local"}, 2, func(_ context.Context, node string) (any, error) {
		calls.Add(1)
		if node == "west" {
			return "stale", nil
		}
		return "current", nil
	}, func(left, right any) bool {
		return left.(string) == right.(string)
	})
	if err != nil {
		t.Fatalf("ExecuteReadQuorum() error = %v, want nil", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("read callback calls = %d, want 3", calls.Load())
	}
	if result.Value != "current" {
		t.Fatalf("read value = %#v, want current", result.Value)
	}
	if result.Decision.Total != 3 || result.Decision.Acknowledged != 2 || result.Decision.Required != 2 || !result.Decision.Satisfied {
		t.Fatalf("read decision = %#v, want total=3 acknowledged=2 required=2 satisfied", result.Decision)
	}
	if len(result.Attempts) != 3 || result.Attempts[0].Node != "east" || !result.Attempts[0].Acknowledged || result.Attempts[1].Node != "west" || result.Attempts[1].Error != "" {
		t.Fatalf("read attempts = %#v, want deterministic successful attempts", result.Attempts)
	}
}

func TestExecuteReadQuorumRejectsInconsistentAndInsufficientResults(t *testing.T) {
	_, err := hatReplication.ExecuteReadQuorum(context.Background(), []string{"a", "b", "c"}, 2, func(_ context.Context, node string) (any, error) {
		return node, nil
	}, func(left, right any) bool {
		return left == right
	})
	if !errors.Is(err, hatReplication.ErrReadQuorumInconsistent) {
		t.Fatalf("inconsistent error = %v, want ErrReadQuorumInconsistent", err)
	}

	_, err = hatReplication.ExecuteReadQuorum(context.Background(), []string{"a", "b", "c"}, 2, func(_ context.Context, node string) (any, error) {
		if node != "b" {
			return nil, errors.New("offline")
		}
		return "only-value", nil
	}, nil)
	if !errors.Is(err, hatReplication.ErrReadQuorumUnsatisfied) {
		t.Fatalf("insufficient error = %v, want ErrReadQuorumUnsatisfied", err)
	}
}

func TestExecuteReadQuorumUsesDeepEqualityAndRejectsInvalidOrCanceledInput(t *testing.T) {
	result, err := hatReplication.ExecuteReadQuorum(context.Background(), []string{"a", "b"}, 2, func(_ context.Context, _ string) (any, error) {
		return []int{1, 2, 3}, nil
	}, nil)
	if err != nil || result.Value == nil || !result.Decision.Satisfied {
		t.Fatalf("deep-equal read = %#v, %v, want satisfied", result, err)
	}

	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = hatReplication.ExecuteReadQuorum(ctx, []string{"a"}, 1, func(context.Context, string) (any, error) {
		calls.Add(1)
		return "unexpected", nil
	}, nil)
	if !errors.Is(err, hatReplication.ErrReadQuorumContextCanceled) || calls.Load() != 0 {
		t.Fatalf("canceled read = %v with %d calls, want cancellation and no callback", err, calls.Load())
	}

	if _, err := hatReplication.ExecuteReadQuorum(nil, []string{"a"}, 1, func(context.Context, string) (any, error) { return nil, nil }, nil); !errors.Is(err, hatReplication.ErrReadQuorumExecutorInvalid) {
		t.Fatalf("nil context error = %v, want ErrReadQuorumExecutorInvalid", err)
	}
}
