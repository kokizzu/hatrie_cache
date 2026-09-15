package hatReplication_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestExecuteReadQuorumSingleNodeSuccess(t *testing.T) {
	var calls int
	result, err := hatReplication.ExecuteReadQuorum(context.Background(), []string{"east"}, 1, func(_ context.Context, node string) (any, error) {
		calls++
		if node != "east" {
			t.Fatalf("node = %q, want east", node)
		}
		return "current", nil
	}, nil)
	if err != nil {
		t.Fatalf("ExecuteReadQuorum() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("read calls = %d, want 1", calls)
	}
	if result.Value != "current" || !result.Decision.Satisfied || result.Decision.Acknowledged != 1 {
		t.Fatalf("result = %#v, want one acknowledged current value", result)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Node != "east" || !result.Attempts[0].Acknowledged {
		t.Fatalf("attempts = %#v, want one acknowledged east attempt", result.Attempts)
	}
}

func TestExecuteReadQuorumSingleNodeFailureKeepsUnsatisfiedContract(t *testing.T) {
	failure := errors.New("unavailable")
	result, err := hatReplication.ExecuteReadQuorum(context.Background(), []string{"east"}, 1, func(context.Context, string) (any, error) {
		return nil, failure
	}, nil)
	if !errors.Is(err, hatReplication.ErrReadQuorumUnsatisfied) {
		t.Fatalf("error = %v, want ErrReadQuorumUnsatisfied", err)
	}
	if result.Decision.Satisfied || result.Decision.Acknowledged != 0 || result.Decision.Total != 1 || result.Decision.Required != 1 {
		t.Fatalf("decision = %#v, want one required and zero acknowledged", result.Decision)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Node != "east" || result.Attempts[0].Acknowledged || result.Attempts[0].Error != failure.Error() {
		t.Fatalf("attempts = %#v, want failed east attempt", result.Attempts)
	}
}
