package hatCache

import (
	"path/filepath"
	"testing"
)

var tu34SpaceSyncPolicyBenchmarkSink CacheCommandResponse

func BenchmarkTU34CommandJournal(b *testing.B) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
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
		tu34SpaceSyncPolicyBenchmarkSink = journal.ExecuteCommand(trie, request)
	}
}
