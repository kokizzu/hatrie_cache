package hatCache

import (
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func BenchmarkCommandJournalIdempotencyEncoding(b *testing.B) {
	for _, format := range []CommandJournalFormat{CommandJournalFormatBinary, CommandJournalFormatJSON} {
		for _, test := range []struct {
			name  string
			entry commandJournalEntry
		}{
			{
				name: "Unkeyed",
				entry: commandJournalEntry{
					Sequence: 1,
					Request: CacheCommandRequest{
						Command: "INC",
						Key:     "benchmark-counter",
						Value:   "1",
					},
				},
			},
			{
				name: "Keyed",
				entry: commandJournalEntry{
					Sequence: 1,
					Request: CacheCommandRequest{
						Command:        "INC",
						Key:            "benchmark-counter",
						Value:          "1",
						IdempotencyKey: "benchmark-retry",
					},
					IdempotencyFingerprint: func() []byte {
						check, _ := newCommandIdempotencyCheck(CacheCommandRequest{
							Command:        "INC",
							Key:            "benchmark-counter",
							Value:          "1",
							IdempotencyKey: "benchmark-retry",
						})
						return check.fingerprint[:]
					}(),
				},
			},
		} {
			b.Run(string(format)+"/"+test.name, func(b *testing.B) {
				data, err := marshalCommandJournalEntry(test.entry, format)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					if _, err := marshalCommandJournalEntry(test.entry, format); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(data)), "bytes/record")
			})
		}
	}
}

func BenchmarkCommandJournalIdempotencyRetry(b *testing.B) {
	for _, test := range []struct {
		name     string
		capacity int
	}{
		{name: "Disabled", capacity: 0},
		{name: "Enabled", capacity: 16},
	} {
		b.Run(test.name, func(b *testing.B) {
			journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
				Format:              CommandJournalFormatBinary,
				GroupCommitMaxBatch: 1,
				IdempotencyCapacity: test.capacity,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer journal.Close()
			journal.syncHook = func() error { return nil }
			trie := CreateHatTrie()
			defer trie.Destroy()

			request := CacheCommandRequest{
				Command:        "INC",
				Key:            "benchmark-counter",
				Value:          "1",
				IdempotencyKey: "benchmark-retry",
			}
			if response := journal.ExecuteCommand(trie, request); !response.OK {
				b.Fatalf("warmup response = %#v", response)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if response := journal.ExecuteCommand(trie, request); !response.OK {
					b.Fatalf("retry response = %#v", response)
				}
			}
		})
	}
}

func BenchmarkCommandJournalIdempotencyGroupCommit(b *testing.B) {
	const groupSize = 8
	for _, test := range []struct {
		name     string
		capacity int
	}{
		{name: "Disabled", capacity: 0},
		{name: "Enabled", capacity: 16},
	} {
		b.Run(test.name, func(b *testing.B) {
			journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
				Format:              CommandJournalFormatBinary,
				GroupCommitMaxBatch: groupSize,
				GroupCommitWindow:   time.Millisecond,
				IdempotencyCapacity: test.capacity,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer journal.Close()
			trie := CreateHatTrie()
			defer trie.Destroy()
			requests := make([]CacheCommandRequest, b.N*groupSize)
			for index := range requests {
				requests[index] = CacheCommandRequest{
					Command:        "INC",
					Key:            "benchmark-group-counter",
					Value:          "1",
					IdempotencyKey: "benchmark-group-" + strconv.Itoa(index),
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for batch := 0; batch < b.N; batch++ {
				var group sync.WaitGroup
				responses := make(chan CacheCommandResponse, groupSize)
				start := batch * groupSize
				for index := 0; index < groupSize; index++ {
					group.Add(1)
					go func(request CacheCommandRequest) {
						defer group.Done()
						responses <- journal.ExecuteCommand(trie, request)
					}(requests[start+index])
				}
				group.Wait()
				close(responses)
				for response := range responses {
					if !response.OK {
						b.Fatalf("group response = %#v", response)
					}
				}
			}
		})
	}
}
