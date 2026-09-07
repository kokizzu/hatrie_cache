package hatReplication_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatReplication"
)

func TestExecuteParallelReplicaReadReturnsFastSuccessAndCancelsLoser(t *testing.T) {
	slowStarted := make(chan struct{})
	slowCanceled := make(chan struct{})
	result, err := hatReplication.ExecuteParallelReplicaRead(context.Background(), []string{"slow", "fast"}, 5*time.Millisecond, func(ctx context.Context, node string) (any, error) {
		if node == "slow" {
			close(slowStarted)
			<-ctx.Done()
			close(slowCanceled)
			return nil, ctx.Err()
		}
		return "value", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Node != "fast" || result.Value != "value" {
		t.Fatalf("result = %#v, want fast/value", result)
	}
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow replica did not start")
	}
	select {
	case <-slowCanceled:
	case <-time.After(time.Second):
		t.Fatal("slow replica was not canceled")
	}
	if len(result.Attempts) != 2 || !result.Attempts[0].Started || !result.Attempts[1].Started || !result.Attempts[1].Succeeded {
		t.Fatalf("attempts = %#v, want both started and fast succeeded", result.Attempts)
	}
}

func TestExecuteParallelReplicaReadFailsOverImmediatelyAfterAnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	result, err := hatReplication.ExecuteParallelReplicaRead(ctx, []string{"failed", "available"}, time.Hour, func(_ context.Context, node string) (any, error) {
		if node == "failed" {
			return nil, errors.New("connection refused")
		}
		return "value", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Node != "available" || result.Value != "value" {
		t.Fatalf("result = %#v, want available/value", result)
	}
}

func TestExecuteParallelReplicaReadRunsAllAtZeroHedgeDelayAndPreservesFailureOrder(t *testing.T) {
	wantErr := errors.New("replica unavailable")
	result, err := hatReplication.ExecuteParallelReplicaRead(context.Background(), []string{"a", "b", "c"}, 0, func(context.Context, string) (any, error) {
		return nil, wantErr
	})
	if !errors.Is(err, hatReplication.ErrParallelReplicaReadFailed) {
		t.Fatalf("error = %v, want parallel read failure", err)
	}
	if len(result.Attempts) != 3 {
		t.Fatalf("attempt count = %d, want 3", len(result.Attempts))
	}
	for index, attempt := range result.Attempts {
		if attempt.Node != []string{"a", "b", "c"}[index] || !attempt.Started || !attempt.Completed || attempt.Succeeded || attempt.Error != wantErr.Error() {
			t.Fatalf("attempt %d = %#v", index, attempt)
		}
	}
}

func TestExecuteParallelReplicaReadValidatesInputsAndCancellation(t *testing.T) {
	cases := []struct {
		name  string
		nodes []string
		delay time.Duration
		read  hatReplication.ParallelReplicaReadFunc
		want  error
	}{
		{name: "empty nodes", want: hatReplication.ErrParallelReplicaReadInvalid},
		{name: "blank node", nodes: []string{""}, want: hatReplication.ErrParallelReplicaReadInvalid},
		{name: "duplicate node", nodes: []string{"a", "a"}, want: hatReplication.ErrParallelReplicaReadInvalid},
		{name: "negative delay", nodes: []string{"a"}, delay: -time.Nanosecond, want: hatReplication.ErrParallelReplicaReadInvalid},
		{name: "nil callback", nodes: []string{"a"}, want: hatReplication.ErrParallelReplicaReadInvalid},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := hatReplication.ExecuteParallelReplicaRead(context.Background(), test.nodes, test.delay, test.read)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := hatReplication.ExecuteParallelReplicaRead(canceled, []string{"a"}, 0, func(context.Context, string) (any, error) {
		t.Fatal("canceled callback ran")
		return nil, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v, want context canceled", err)
	}
}
