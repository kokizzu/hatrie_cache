package hatCache

import (
	"testing"
	"time"
)

func BenchmarkTU34SynchronousBaseline(b *testing.B) {
	benchmarkTU34Policy(b, "", false)
}

func BenchmarkTU34PeriodicSpace(b *testing.B) {
	benchmarkTU34Policy(b, CommandJournalSyncPeriodic, false)
}

func BenchmarkTU34DisabledSpace(b *testing.B) {
	benchmarkTU34Policy(b, CommandJournalSyncDisabled, false)
}

func BenchmarkTU34SynchronousDisk(b *testing.B) {
	benchmarkTU34Policy(b, "", true)
}

func BenchmarkTU34PeriodicDisk(b *testing.B) {
	benchmarkTU34Policy(b, CommandJournalSyncPeriodic, true)
}

func BenchmarkTU34DisabledDisk(b *testing.B) {
	benchmarkTU34Policy(b, CommandJournalSyncDisabled, true)
}

func benchmarkTU34Policy(b *testing.B, mode CommandJournalSyncMode, realSync bool) {
	options := CommandJournalOptions{GroupCommitMaxBatch: 1}
	if mode != "" {
		options.SyncPolicy = CommandJournalSyncPolicy{
			PeriodicInterval: time.Hour,
			Rules:            []CommandJournalSyncPolicyRule{{SpacePrefix: "region:", Mode: mode}},
		}
	}
	journal, err := OpenCommandJournalWithOptions(b.TempDir()+"/commands.journal", CommandJournalOptions{
		GroupCommitMaxBatch: options.GroupCommitMaxBatch,
		SyncPolicy:          options.SyncPolicy,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	trie := CreateHatTrie()
	defer trie.Destroy()
	request := CacheCommandRequest{Command: "SET", Key: "region:durable:key", Value: "value"}
	syncs := 0
	if realSync {
		journal.syncHook = func() error {
			syncs++
			return journal.file.Sync()
		}
	} else {
		journal.syncHook = func() error {
			syncs++
			return nil
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if response := journal.ExecuteCommand(trie, request); !response.OK {
			b.Fatal(response.Message)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(syncs)/float64(b.N), "syncs/op")
}
