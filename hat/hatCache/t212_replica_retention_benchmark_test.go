package hatCache

import (
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkT212ReplicaRetentionModes(b *testing.B) {
	for _, test := range []struct {
		name     string
		capacity int
		register bool
		ack      bool
	}{
		{name: "Disabled"},
		{name: "EnabledLagging", capacity: 2, register: true},
		{name: "EnabledCaughtUp", capacity: 2, register: true, ack: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
				Format:                   CommandJournalFormatBinary,
				GroupCommitMaxBatch:      1,
				SegmentMaxBytes:          256,
				RetainedSegments:         1,
				ReplicaRetentionCapacity: test.capacity,
			})
			if err != nil {
				b.Fatal(err)
			}
			trie := CreateHatTrie()
			b.Cleanup(func() {
				_ = journal.Close()
				trie.Destroy()
			})
			if test.register {
				if err := journal.RegisterReplicaRetention("node-b", 0); err != nil {
					b.Fatal(err)
				}
				if err := journal.RegisterReplicaRetention("node-c", 0); err != nil {
					b.Fatal(err)
				}
			}
			value := strings.Repeat("value", 64)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				response := journal.ExecuteCommand(trie, CacheCommandRequest{
					Command: "SETSTR",
					Key:     "t212-benchmark",
					Value:   value,
				})
				if !response.OK {
					b.Fatal(response.Message)
				}
				if test.ack {
					sequence := journal.Sequence()
					if err := journal.AcknowledgeReplicaThrough("node-b", sequence); err != nil {
						b.Fatal(err)
					}
					if err := journal.AcknowledgeReplicaThrough("node-c", sequence); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			segments, err := listCommandJournalSegments(journal.path)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(len(segments)), "segments")
		})
	}
}
