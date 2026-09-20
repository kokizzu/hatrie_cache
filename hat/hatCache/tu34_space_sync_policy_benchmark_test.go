package hatCache

import (
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatJournal"
)

func BenchmarkTU34PeriodicSpaceCommandJournal(b *testing.B) {
	registry, err := hatJournal.NewSpaceSyncPolicyRegistry(hatJournal.SpaceSyncPolicyOptions{
		DefaultPolicy: hatJournal.SpaceSyncPolicyPeriodic,
	})
	if err != nil {
		b.Fatal(err)
	}
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		SpaceSyncPolicies:   registry,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	journal.syncHook = func() error { return nil }
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	request := CacheCommandRequest{Command: "SETSTR", Key: "benchmark:key", Value: "value"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tu34SpaceSyncPolicyBenchmarkSink = journal.ExecuteCommandInSpace(trie, "orders", request)
	}
}

func BenchmarkTU34DisabledSpaceCommandJournal(b *testing.B) {
	registry, err := hatJournal.NewSpaceSyncPolicyRegistry(hatJournal.SpaceSyncPolicyOptions{
		DefaultPolicy: hatJournal.SpaceSyncPolicyDisabled,
	})
	if err != nil {
		b.Fatal(err)
	}
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		SpaceSyncPolicies:   registry,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	journal.syncHook = func() error { return nil }
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	request := CacheCommandRequest{Command: "SETSTR", Key: "benchmark:key", Value: "value"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tu34SpaceSyncPolicyBenchmarkSink = journal.ExecuteCommandInSpace(trie, "ephemeral", request)
	}
}
