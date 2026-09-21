package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestExecuteClusterWriteCommitPreparesAllBeforeCommitting(t *testing.T) {
	var mu sync.Mutex
	prepared := make(map[string]bool)
	committed := make(map[string]bool)
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-1", Sequence: 42, FenceToken: 7}
	result, err := ExecuteClusterWriteCommit(context.Background(), []string{"node-a", "node-b", "node-c"}, proposal,
		func(_ context.Context, node string, got ClusterWriteCommitProposal) error {
			if got != proposal {
				t.Errorf("prepare proposal = %#v, want %#v", got, proposal)
			}
			mu.Lock()
			prepared[node] = true
			mu.Unlock()
			return nil
		},
		func(_ context.Context, node string, got ClusterWriteCommitProposal) error {
			mu.Lock()
			defer mu.Unlock()
			if !prepared[node] {
				return fmt.Errorf("%s committed before prepare", node)
			}
			if got != proposal {
				return fmt.Errorf("commit proposal = %#v, want %#v", got, proposal)
			}
			committed[node] = true
			return nil
		},
		func(context.Context, string, ClusterWriteCommitProposal) error {
			t.Fatal("abort callback invoked after successful commit")
			return nil
		},
	)
	if err != nil {
		t.Fatalf("ExecuteClusterWriteCommit() error = %v", err)
	}
	if !result.Prepared || !result.Committed || result.OutcomeUnknown || result.PreparedCount != 3 || result.CommittedCount != 3 || result.AbortedCount != 0 {
		t.Fatalf("result = %#v, want fully committed result", result)
	}
	if len(result.Attempts) != 3 || !result.Attempts[0].Committed || !result.Attempts[1].Committed || !result.Attempts[2].Committed {
		t.Fatalf("attempts = %#v, want input-order committed attempts", result.Attempts)
	}
	if len(committed) != 3 {
		t.Fatalf("committed nodes = %#v, want all nodes", committed)
	}
}

func TestExecuteClusterWriteCommitAbortsPreparedNodesWhenPrepareFails(t *testing.T) {
	var mu sync.Mutex
	aborted := make(map[string]bool)
	result, err := ExecuteClusterWriteCommit(context.Background(), []string{"node-a", "node-b", "node-c"}, ClusterWriteCommitProposal{TransactionID: "tx-prepare-fail"},
		func(_ context.Context, node string, _ ClusterWriteCommitProposal) error {
			if node == "node-b" {
				return errors.New("prepare rejected")
			}
			return nil
		},
		func(context.Context, string, ClusterWriteCommitProposal) error {
			t.Fatal("commit callback invoked after prepare failure")
			return nil
		},
		func(_ context.Context, node string, _ ClusterWriteCommitProposal) error {
			mu.Lock()
			aborted[node] = true
			mu.Unlock()
			return nil
		},
	)
	if !errors.Is(err, ErrClusterWriteCommitPrepareFailed) {
		t.Fatalf("error = %v, want prepare failure", err)
	}
	if result.Prepared || result.Committed || result.OutcomeUnknown || result.PreparedCount != 2 || result.AbortedCount != 2 {
		t.Fatalf("result = %#v, want two prepared nodes aborted", result)
	}
	if len(aborted) != 2 || aborted["node-b"] {
		t.Fatalf("aborted nodes = %#v, want only prepared nodes", aborted)
	}
}

func TestExecuteClusterWriteCommitReportsUnknownOutcomeAfterCommitFailure(t *testing.T) {
	result, err := ExecuteClusterWriteCommit(context.Background(), []string{"node-a", "node-b", "node-c"}, ClusterWriteCommitProposal{TransactionID: "tx-commit-fail"},
		func(context.Context, string, ClusterWriteCommitProposal) error { return nil },
		func(_ context.Context, node string, _ ClusterWriteCommitProposal) error {
			if node == "node-b" {
				return errors.New("commit transport failed")
			}
			return nil
		},
		func(context.Context, string, ClusterWriteCommitProposal) error {
			t.Fatal("abort callback invoked after commit phase started")
			return nil
		},
	)
	if !errors.Is(err, ErrClusterWriteCommitOutcomeUnknown) {
		t.Fatalf("error = %v, want unknown outcome", err)
	}
	if !result.Prepared || result.Committed || !result.OutcomeUnknown || result.PreparedCount != 3 || result.CommittedCount != 2 || result.AbortedCount != 0 {
		t.Fatalf("result = %#v, want indeterminate commit outcome", result)
	}
}

func TestExecuteClusterWriteCommitRejectsInvalidInput(t *testing.T) {
	callback := func(context.Context, string, ClusterWriteCommitProposal) error { return nil }
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-valid"}
	for name, test := range map[string]struct {
		ctx      context.Context
		nodes    []string
		proposal ClusterWriteCommitProposal
		prepare  ClusterWriteCommitPrepareFunc
		commit   ClusterWriteCommitCommitFunc
		abort    ClusterWriteCommitAbortFunc
	}{
		"nil context":       {nodes: []string{"node-a"}, proposal: proposal, prepare: callback, commit: callback, abort: callback},
		"empty nodes":       {ctx: context.Background(), proposal: proposal, prepare: callback, commit: callback, abort: callback},
		"blank transaction": {ctx: context.Background(), nodes: []string{"node-a"}, prepare: callback, commit: callback, abort: callback},
		"duplicate nodes":   {ctx: context.Background(), nodes: []string{"node-a", "node-a"}, proposal: proposal, prepare: callback, commit: callback, abort: callback},
		"too many nodes":    {ctx: context.Background(), nodes: make([]string, MaxClusterWriteCommitNodes+1), proposal: proposal, prepare: callback, commit: callback, abort: callback},
		"nil prepare":       {ctx: context.Background(), nodes: []string{"node-a"}, proposal: proposal, commit: callback, abort: callback},
		"nil commit":        {ctx: context.Background(), nodes: []string{"node-a"}, proposal: proposal, prepare: callback, abort: callback},
		"nil abort":         {ctx: context.Background(), nodes: []string{"node-a"}, proposal: proposal, prepare: callback, commit: callback},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ExecuteClusterWriteCommit(test.ctx, test.nodes, test.proposal, test.prepare, test.commit, test.abort); !errors.Is(err, ErrClusterWriteCommitInvalid) {
				t.Fatalf("error = %v, want invalid input", err)
			}
		})
	}
}
