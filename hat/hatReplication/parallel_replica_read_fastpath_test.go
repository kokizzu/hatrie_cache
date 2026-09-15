package hatReplication_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestExecuteParallelReplicaReadSingleNodeSuccess(t *testing.T) {
	called := ""
	result, err := hatReplication.ExecuteParallelReplicaRead(context.Background(), []string{" node-a "}, 0, func(_ context.Context, node string) (any, error) {
		called = node
		return "value", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called != "node-a" || result.Node != "node-a" || result.Value != "value" {
		t.Fatalf("result = %#v, called=%q", result, called)
	}
	if len(result.Attempts) != 1 || !result.Attempts[0].Started || !result.Attempts[0].Completed || !result.Attempts[0].Succeeded || result.Attempts[0].Error != "" {
		t.Fatalf("attempts = %#v", result.Attempts)
	}
}

func TestExecuteParallelReplicaReadSingleNodeFailureKeepsFailureContract(t *testing.T) {
	wantErr := errors.New("unavailable")
	result, err := hatReplication.ExecuteParallelReplicaRead(context.Background(), []string{"node-a"}, 0, func(context.Context, string) (any, error) {
		return nil, wantErr
	})
	if !errors.Is(err, hatReplication.ErrParallelReplicaReadFailed) {
		t.Fatalf("error = %v, want ErrParallelReplicaReadFailed", err)
	}
	if len(result.Attempts) != 1 || !result.Attempts[0].Started || !result.Attempts[0].Completed || result.Attempts[0].Succeeded || result.Attempts[0].Error != wantErr.Error() {
		t.Fatalf("result = %#v", result)
	}
}
