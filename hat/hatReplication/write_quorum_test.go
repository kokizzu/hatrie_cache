package hatReplication_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestExecuteWriteQuorumRunsTargetsAndReportsSatisfiedDecision(t *testing.T) {
	boom := errors.New("replica unavailable")
	result, err := hatReplication.ExecuteWriteQuorum(context.Background(), []string{"local", "east", "west"}, 2, func(_ context.Context, node string) error {
		if node == "east" {
			return boom
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteWriteQuorum() error = %v, want nil", err)
	}
	if !result.Decision.Satisfied || result.Decision.Total != 3 || result.Decision.Acknowledged != 2 || result.Decision.Required != 2 {
		t.Fatalf("decision = %#v, want 2/3 satisfied", result.Decision)
	}
	if len(result.Attempts) != 3 || result.Attempts[0].Node != "local" || !result.Attempts[0].Acknowledged || result.Attempts[1].Acknowledged || result.Attempts[1].Error == "" || !result.Attempts[2].Acknowledged {
		t.Fatalf("attempts = %#v, want ordered target outcomes", result.Attempts)
	}
}

func TestExecuteWriteQuorumReportsUnsatisfiedAndRunsAllTargets(t *testing.T) {
	result, err := hatReplication.ExecuteWriteQuorum(context.Background(), []string{"a", "b", "c"}, 3, func(_ context.Context, node string) error {
		if node != "a" {
			return errors.New("unavailable")
		}
		return nil
	})
	if !errors.Is(err, hatReplication.ErrWriteQuorumUnsatisfied) {
		t.Fatalf("unsatisfied error = %v, want ErrWriteQuorumUnsatisfied", err)
	}
	if result.Decision.Satisfied || result.Decision.Acknowledged != 1 || len(result.Attempts) != 3 {
		t.Fatalf("unsatisfied result = %#v, want one acknowledgement and all attempts", result)
	}
}

func TestExecuteWriteQuorumRejectsInvalidInputsAndCanceledContext(t *testing.T) {
	for name, test := range map[string]struct {
		nodes    []string
		required int
		write    hatReplication.WriteQuorumWriteFunc
	}{
		"empty nodes":       {required: 1, write: func(context.Context, string) error { return nil }},
		"zero required":     {nodes: []string{"a"}, write: func(context.Context, string) error { return nil }},
		"required too high": {nodes: []string{"a"}, required: 2, write: func(context.Context, string) error { return nil }},
		"duplicate node":    {nodes: []string{"a", "a"}, required: 1, write: func(context.Context, string) error { return nil }},
		"nil callback":      {nodes: []string{"a"}, required: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatReplication.ExecuteWriteQuorum(context.Background(), test.nodes, test.required, test.write); !errors.Is(err, hatReplication.ErrWriteQuorumExecutorInvalid) {
				t.Fatalf("error = %v, want ErrWriteQuorumExecutorInvalid", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := hatReplication.ExecuteWriteQuorum(ctx, []string{"a", "b"}, 1, func(context.Context, string) error {
		t.Fatal("canceled quorum invoked write callback")
		return nil
	})
	if !errors.Is(err, hatReplication.ErrWriteQuorumContextCanceled) || result.Decision.Acknowledged != 0 {
		t.Fatalf("canceled result = %#v/%v, want canceled with no acknowledgements", result, err)
	}
}
