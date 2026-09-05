package hatCache

import (
	"errors"
	"testing"
)

func TestRunAtomicCallbackCommitsAllStagedCommands(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	response, err := trie.RunAtomic(func(batch *AtomicCommandBatch) error {
		if err := batch.Add(CacheCommandRequest{Command: "SETSTR", Key: "atomic:first", Value: "one"}); err != nil {
			return err
		}
		return batch.Add(CacheCommandRequest{Command: "SETSTR", Key: "atomic:second", Value: "two"})
	})
	if err != nil {
		t.Fatalf("RunAtomic() error = %v", err)
	}
	if !response.OK || len(response.Responses) != 2 {
		t.Fatalf("RunAtomic() response = %+v", response)
	}
	for key, want := range map[string]string{"atomic:first": "one", "atomic:second": "two"} {
		got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: key})
		if !got.OK || got.Value != want {
			t.Fatalf("GETSTR %q response = %+v, want value %q", key, got, want)
		}
	}
}

func TestRunAtomicCallbackErrorDoesNotMutate(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	wantErr := errors.New("abort atomic callback")
	response, err := trie.RunAtomic(func(batch *AtomicCommandBatch) error {
		if err := batch.Add(CacheCommandRequest{Command: "SETSTR", Key: "atomic:aborted", Value: "must not persist"}); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("RunAtomic() error = %v, want %v", err, wantErr)
	}
	if response.OK || response.Message != "" {
		t.Fatalf("RunAtomic() response after callback error = %+v, want zero response", response)
	}
	got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "atomic:aborted"})
	if !got.OK || got.Value != "" || got.Message != "key not found" {
		t.Fatalf("aborted key response = %+v", got)
	}
}

func TestRunAtomicCommandFailureRollsBackEarlierCommands(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	response, err := trie.RunAtomic(func(batch *AtomicCommandBatch) error {
		if err := batch.Add(CacheCommandRequest{Command: "SETSTR", Key: "atomic:rollback", Value: "must roll back"}); err != nil {
			return err
		}
		return batch.Add(CacheCommandRequest{Command: "UNKNOWN_ATOMIC_COMMAND", Key: "atomic:failure"})
	})
	if err == nil {
		t.Fatalf("RunAtomic() error = nil, response = %+v", response)
	}
	if response.OK || len(response.Responses) != 2 {
		t.Fatalf("RunAtomic() failure response = %+v", response)
	}
	got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "atomic:rollback"})
	if !got.OK || got.Value != "" || got.Message != "key not found" {
		t.Fatalf("rolled-back key response = %+v", got)
	}
}

func TestRunAtomicCopiesMutableCommandPayloads(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	pairs := map[string]any{"field": "original"}
	response, err := trie.RunAtomic(func(batch *AtomicCommandBatch) error {
		err := batch.Add(CacheCommandRequest{Command: "PUTMAP", Key: "atomic:owned", Pairs: pairs})
		pairs["field"] = "mutated after Add"
		return err
	})
	if err != nil || !response.OK {
		t.Fatalf("RunAtomic() response = %+v, error = %v", response, err)
	}
	value, ok, err := trie.PeekMapChecked("atomic:owned", "field")
	if err != nil || !ok || value != "original" {
		t.Fatalf("PeekMapChecked() = (%v, %v, %v), want original value", value, ok, err)
	}
}
