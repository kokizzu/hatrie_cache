package hatCache

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func newSpaceSyncPolicyTestJournal(t *testing.T, groupCommit bool) (*CommandJournal, *HatTrie, *atomic.Uint64) {
	t.Helper()
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)
	options := CommandJournalOptions{GroupCommitMaxBatch: 1}
	if groupCommit {
		options = CommandJournalOptions{
			GroupCommitWindow:   time.Millisecond,
			GroupCommitMaxBatch: 8,
		}
	}
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	var syncs atomic.Uint64
	journal.syncHook = func() error {
		syncs.Add(1)
		return nil
	}
	return journal, trie, &syncs
}

func executeSpaceSyncPolicySet(t *testing.T, journal *CommandJournal, trie *HatTrie, key, value string) {
	t.Helper()
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: key, Value: value})
	if !response.OK {
		t.Fatalf("SETSTR %s response = %#v, want success", key, response)
	}
}

func TestJournalSpaceSyncPolicyDefaultsToImmediate(t *testing.T) {
	journal, trie, syncs := newSpaceSyncPolicyTestJournal(t, false)

	executeSpaceSyncPolicySet(t, journal, trie, "default", "one")
	executeSpaceSyncPolicySet(t, journal, trie, "default", "two")
	if got := syncs.Load(); got != 2 {
		t.Fatalf("default sync count = %d, want 2", got)
	}
	if policies := journal.SpaceSyncPolicies(); len(policies) != 0 {
		t.Fatalf("default policies = %#v, want no overrides", policies)
	}
}

func TestJournalSpaceSyncPolicyPeriodicAndDisabled(t *testing.T) {
	journal, trie, syncs := newSpaceSyncPolicyTestJournal(t, false)

	if err := journal.SetSpaceSyncPolicy("periodic", CommandJournalSpaceSyncPolicy{
		Mode:  CommandJournalSpaceSyncPeriodic,
		Every: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if err := journal.SetSpaceSyncPolicy("disabled", CommandJournalSpaceSyncPolicy{
		Mode: CommandJournalSpaceSyncDisabled,
	}); err != nil {
		t.Fatal(err)
	}

	for index := 0; index < 2; index++ {
		executeSpaceSyncPolicySet(t, journal, trie, "periodic", "value")
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("periodic sync count before cadence = %d, want 0", got)
	}
	executeSpaceSyncPolicySet(t, journal, trie, "periodic", "value")
	if got := syncs.Load(); got != 1 {
		t.Fatalf("periodic sync count at cadence = %d, want 1", got)
	}
	executeSpaceSyncPolicySet(t, journal, trie, "disabled", "value")
	if got := syncs.Load(); got != 1 {
		t.Fatalf("disabled sync count = %d, want unchanged 1", got)
	}
	if err := journal.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := syncs.Load(); got != 2 {
		t.Fatalf("manual sync count = %d, want 2", got)
	}
	executeSpaceSyncPolicySet(t, journal, trie, "default", "value")
	if got := syncs.Load(); got != 3 {
		t.Fatalf("default override sync count = %d, want 3", got)
	}

	policies := journal.SpaceSyncPolicies()
	if policies["periodic"].Every != 3 || policies["periodic"].Mode != CommandJournalSpaceSyncPeriodic {
		t.Fatalf("periodic policy = %#v, want every 3", policies["periodic"])
	}
	if policies["disabled"].Mode != CommandJournalSpaceSyncDisabled {
		t.Fatalf("disabled policy = %#v, want disabled", policies["disabled"])
	}
}

func TestJournalSpaceSyncPolicyValidation(t *testing.T) {
	journal, _, _ := newSpaceSyncPolicyTestJournal(t, false)

	for _, test := range []struct {
		name   string
		space  string
		policy CommandJournalSpaceSyncPolicy
	}{
		{name: "empty space", space: " ", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncDisabled}},
		{name: "unknown mode", space: "space", policy: CommandJournalSpaceSyncPolicy{Mode: "unknown"}},
		{name: "periodic zero", space: "space", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncPeriodic}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := journal.SetSpaceSyncPolicy(test.space, test.policy); err == nil {
				t.Fatalf("SetSpaceSyncPolicy(%q, %#v) succeeded, want error", test.space, test.policy)
			}
		})
	}
}

func TestJournalSpaceSyncPolicyRestoresPeriodicCadenceAfterRejectedCommand(t *testing.T) {
	journal, trie, syncs := newSpaceSyncPolicyTestJournal(t, false)
	if err := journal.SetSpaceSyncPolicy("periodic", CommandJournalSpaceSyncPolicy{
		Mode:  CommandJournalSpaceSyncPeriodic,
		Every: 2,
	}); err != nil {
		t.Fatal(err)
	}

	executeSpaceSyncPolicySet(t, journal, trie, "periodic", "one")
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETINT", Key: "periodic", Value: "not-an-int"})
	if response.OK {
		t.Fatalf("rejected SETINT response = %#v, want error", response)
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("rejected command sync count = %d, want 0", got)
	}
	executeSpaceSyncPolicySet(t, journal, trie, "periodic", "three")
	if got := syncs.Load(); got != 1 {
		t.Fatalf("post-rejection cadence sync count = %d, want 1", got)
	}
}

func TestJournalSpaceSyncPolicyAppliesToGroupCommit(t *testing.T) {
	journal, trie, syncs := newSpaceSyncPolicyTestJournal(t, true)
	if err := journal.SetSpaceSyncPolicy("disabled", CommandJournalSpaceSyncPolicy{
		Mode: CommandJournalSpaceSyncDisabled,
	}); err != nil {
		t.Fatal(err)
	}

	for index := 0; index < 3; index++ {
		submission, err := journal.submitAsyncCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "disabled",
			Value:   "value",
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := submission.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("disabled group sync count = %d, want 0", got)
	}

	submission, err := journal.submitAsyncCommand(trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "default",
		Value:   "value",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("default group sync count = %d, want 1", got)
	}
}

func TestJournalSpaceSyncPolicyAppliesToRecordBatch(t *testing.T) {
	journal, trie, syncs := newSpaceSyncPolicyTestJournal(t, false)
	if err := journal.SetSpaceSyncPolicy("batch", CommandJournalSpaceSyncPolicy{
		Mode: CommandJournalSpaceSyncDisabled,
	}); err != nil {
		t.Fatal(err)
	}

	applied, response := journal.executeJournalRecordsBatchWithScalarBatch(trie, []CommandJournalRecord{
		{Request: CacheCommandRequest{Command: "SETSTR", Key: "batch", Value: "one"}},
		{Request: CacheCommandRequest{Command: "SETSTR", Key: "batch", Value: "two"}},
	}, false)
	if applied != 2 || !response.OK {
		t.Fatalf("disabled record batch = %d/%#v, want 2/success", applied, response)
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("disabled record batch sync count = %d, want 0", got)
	}

	if err := journal.SetSpaceSyncPolicy("batch", CommandJournalSpaceSyncPolicy{
		Mode:  CommandJournalSpaceSyncPeriodic,
		Every: 2,
	}); err != nil {
		t.Fatal(err)
	}
	applied, response = journal.executeJournalRecordsBatchWithScalarBatch(trie, []CommandJournalRecord{
		{Request: CacheCommandRequest{Command: "SETSTR", Key: "batch", Value: "three"}},
		{Request: CacheCommandRequest{Command: "SETSTR", Key: "batch", Value: "four"}},
	}, false)
	if applied != 2 || !response.OK {
		t.Fatalf("periodic record batch = %d/%#v, want 2/success", applied, response)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("periodic record batch sync count = %d, want 1", got)
	}
}

func TestJournalSpaceSyncPolicyAppliesToIdempotentGroupCommit(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitWindow:   time.Millisecond,
		GroupCommitMaxBatch: 8,
		IdempotencyCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	var syncs atomic.Uint64
	journal.syncHook = func() error {
		syncs.Add(1)
		return nil
	}
	if err := journal.SetSpaceSyncPolicy("disabled", CommandJournalSpaceSyncPolicy{
		Mode: CommandJournalSpaceSyncDisabled,
	}); err != nil {
		t.Fatal(err)
	}
	for index, key := range []string{"idempotent-1", "idempotent-2", "idempotent-3"} {
		submission, err := journal.submitAsyncCommand(trie, CacheCommandRequest{
			Command:        "SETSTR",
			Key:            "disabled",
			Value:          "value",
			IdempotencyKey: key,
		}, nil)
		if err != nil {
			t.Fatalf("submit idempotent command %d: %v", index, err)
		}
		if _, err := submission.Wait(context.Background()); err != nil {
			t.Fatalf("wait idempotent command %d: %v", index, err)
		}
	}
	if got := syncs.Load(); got != 0 {
		t.Fatalf("disabled idempotent group sync count = %d, want 0", got)
	}

	submission, err := journal.submitAsyncCommand(trie, CacheCommandRequest{
		Command:        "SETSTR",
		Key:            "default",
		Value:          "value",
		IdempotencyKey: "default-1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("default idempotent group sync count = %d, want 1", got)
	}
}

func BenchmarkJournalSpaceSyncPolicy(b *testing.B) {
	for _, test := range []struct {
		name   string
		policy CommandJournalSpaceSyncPolicy
	}{
		{name: "Immediate"},
		{name: "Periodic64", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncPeriodic, Every: 64}},
		{name: "Disabled", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncDisabled}},
	} {
		b.Run(test.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{GroupCommitMaxBatch: 1})
			if err != nil {
				b.Fatal(err)
			}
			defer journal.Close()
			var syncs uint64
			journal.syncHook = func() error {
				syncs++
				return nil
			}
			if test.policy.Mode != "" {
				if err := journal.SetSpaceSyncPolicy("benchmark", test.policy); err != nil {
					b.Fatal(err)
				}
			}
			request := CacheCommandRequest{Command: "SETSTR", Key: "benchmark", Value: "value"}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if response := journal.ExecuteCommand(trie, request); !response.OK {
					b.Fatal(response.Message)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(syncs), "syncs")
		})
	}
}

func BenchmarkJournalSpaceSyncPolicyActualSync(b *testing.B) {
	for _, test := range []struct {
		name   string
		policy CommandJournalSpaceSyncPolicy
	}{
		{name: "Immediate"},
		{name: "Periodic64", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncPeriodic, Every: 64}},
		{name: "Disabled", policy: CommandJournalSpaceSyncPolicy{Mode: CommandJournalSpaceSyncDisabled}},
	} {
		b.Run(test.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{GroupCommitMaxBatch: 1})
			if err != nil {
				b.Fatal(err)
			}
			defer journal.Close()
			var syncs uint64
			if test.policy.Mode != "" {
				if err := journal.SetSpaceSyncPolicy("benchmark", test.policy); err != nil {
					b.Fatal(err)
				}
			}
			journal.syncHook = func() error {
				syncs++
				return journal.file.Sync()
			}
			request := CacheCommandRequest{Command: "SETSTR", Key: "benchmark", Value: "value"}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if response := journal.ExecuteCommand(trie, request); !response.OK {
					b.Fatal(response.Message)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(syncs), "syncs")
		})
	}
}
