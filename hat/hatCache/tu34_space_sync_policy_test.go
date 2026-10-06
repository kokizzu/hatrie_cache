//go:build tu34

package hatCache

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestTU34SpaceSyncPolicyValidation(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir() + "/commands.journal")
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	for _, test := range []struct {
		name   string
		space  string
		policy CommandJournalSpaceSyncPolicy
		want   error
	}{
		{name: "empty space", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModeSynchronous}, want: ErrCommandJournalSpaceNameRequired},
		{name: "unknown mode", space: "orders", policy: CommandJournalSpaceSyncPolicy{Mode: "unknown"}, want: ErrCommandJournalSpaceSyncMode},
		{name: "periodic interval required", space: "orders", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModePeriodic}, want: ErrCommandJournalSpaceSyncInterval},
		{name: "periodic interval positive", space: "orders", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModePeriodic, Interval: -time.Second}, want: ErrCommandJournalSpaceSyncInterval},
		{name: "synchronous interval forbidden", space: "orders", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModeSynchronous, Interval: time.Second}, want: ErrCommandJournalSpaceSyncInterval},
		{name: "disabled interval forbidden", space: "orders", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModeDisabled, Interval: time.Second}, want: ErrCommandJournalSpaceSyncInterval},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := journal.OpenSpace(test.space, test.policy)
			if !errors.Is(err, test.want) {
				t.Fatalf("OpenSpace() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestTU34SpacePolicyCanBeUpdatedThroughExistingHandle(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir() + "/commands.journal")
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	space, err := journal.OpenSpace("orders", CommandJournalSpaceSyncPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if space.Name() != "orders" || space.SyncPolicy().Mode != CommandJournalSpaceSyncModeSynchronous {
		t.Fatalf("initial space = name %q policy %#v", space.Name(), space.SyncPolicy())
	}
	if err := journal.SetSpaceSyncPolicy("orders", CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModeDisabled}); err != nil {
		t.Fatalf("SetSpaceSyncPolicy() error = %v", err)
	}
	if policy := space.SyncPolicy(); policy.Mode != CommandJournalSpaceSyncModeDisabled {
		t.Fatalf("updated policy = %#v, want disabled", policy)
	}
}

func TestTU34SpaceSyncPoliciesPreserveDurabilityBoundaries(t *testing.T) {
	path := t.TempDir() + "/commands.journal"
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{GroupCommitMaxBatch: 1})
	if err != nil {
		t.Fatal(err)
	}
	var syncCalls atomic.Int32
	journal.syncHook = func() error {
		syncCalls.Add(1)
		return nil
	}

	trie := CreateHatTrie()
	defer trie.Destroy()
	durable, err := journal.OpenSpace("durable", CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModeSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	periodic, err := journal.OpenSpace("periodic", CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModePeriodic, Interval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := journal.OpenSpace("disabled", CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModeDisabled})
	if err != nil {
		t.Fatal(err)
	}

	if response := durable.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "durable", Value: "yes"}); !response.OK {
		t.Fatalf("durable ExecuteCommand() = %#v", response)
	}
	if syncCalls.Load() != 1 {
		t.Fatalf("synchronous sync calls = %d, want 1", syncCalls.Load())
	}

	if response := periodic.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "periodic", Value: "yes"}); !response.OK {
		t.Fatalf("periodic ExecuteCommand() = %#v", response)
	}
	if syncCalls.Load() != 1 {
		t.Fatalf("periodic write sync calls = %d, want unchanged at 1", syncCalls.Load())
	}
	if err := periodic.Flush(); err != nil {
		t.Fatalf("periodic Flush() error = %v", err)
	}
	if syncCalls.Load() != 2 {
		t.Fatalf("periodic Flush() sync calls = %d, want 2", syncCalls.Load())
	}
	if response := periodic.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "periodic-close", Value: "yes"}); !response.OK {
		t.Fatalf("periodic close-flush ExecuteCommand() = %#v", response)
	}

	if response := disabled.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "disabled", Value: "yes"}); !response.OK {
		t.Fatalf("disabled ExecuteCommand() = %#v", response)
	}
	if syncCalls.Load() != 2 {
		t.Fatalf("disabled write sync calls = %d, want unchanged at 2", syncCalls.Load())
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed := CreateHatTrie()
	defer replayed.Destroy()
	if _, err := reopened.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if response := replayed.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "durable"}); !response.OK || response.Value != "yes" {
		t.Fatalf("replayed durable value = %#v, want yes", response)
	}
	if response := replayed.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "periodic"}); !response.OK || response.Value != "yes" {
		t.Fatalf("replayed periodic value = %#v, want yes", response)
	}
	if response := replayed.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "periodic-close"}); !response.OK || response.Value != "yes" {
		t.Fatalf("replayed close-flushed value = %#v, want yes", response)
	}
	if response := replayed.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "disabled"}); response.Value != "" || response.Message != "key not found" {
		t.Fatalf("replayed disabled value = %#v, want key not found", response)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTU34PeriodicSpaceFlushesAutomatically(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(t.TempDir()+"/commands.journal", CommandJournalOptions{GroupCommitMaxBatch: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	var syncCalls atomic.Int32
	journal.syncHook = func() error {
		syncCalls.Add(1)
		return nil
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	space, err := journal.OpenSpace("periodic", CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncModePeriodic, Interval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if response := space.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "periodic", Value: "yes"}); !response.OK {
		t.Fatalf("periodic ExecuteCommand() = %#v", response)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for syncCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if syncCalls.Load() == 0 {
		t.Fatal("periodic space did not flush within 500ms")
	}
}
