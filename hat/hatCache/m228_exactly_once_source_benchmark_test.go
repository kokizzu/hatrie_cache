package hatCache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkM228LegacySourceCheckpointCommit(b *testing.B) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "source.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = journal.Close() })
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	store := &m228SourceCheckpointStore{}
	coordinator, err := NewCommandJournalSourceCheckpointCoordinator(journal, store)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETINT",
			Key:     fmt.Sprintf("legacy:%d", index),
			Value:   "1",
		})
		if !response.OK {
			b.Fatal(response.Message)
		}
		if _, err := coordinator.Commit(context.Background(), "source", []byte{byte(index), byte(index >> 8)}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM228ExactlyOnceSourceApply(b *testing.B) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "source.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		IdempotencyCapacity: 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = journal.Close() })
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	coordinator, err := NewCommandJournalExactlyOnceSourceCoordinator(journal, &m228SourceCheckpointStore{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, err := coordinator.ApplyBatch(context.Background(), trie, CommandJournalExactlyOnceSourceBatch{
			SourceID:      "source",
			TransactionID: fmt.Sprintf("tx-%d", index),
			Offset:        []byte{byte(index), byte(index >> 8)},
			Commands: []CacheCommandRequest{{
				Command: "SETINT",
				Key:     fmt.Sprintf("exact:%d", index),
				Value:   "1",
			}},
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM228ExactlyOnceSourceRetry(b *testing.B) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "source.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
		IdempotencyCapacity: 8,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = journal.Close() })
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	coordinator, err := NewCommandJournalExactlyOnceSourceCoordinator(journal, &m228SourceCheckpointStore{})
	if err != nil {
		b.Fatal(err)
	}
	batch := CommandJournalExactlyOnceSourceBatch{
		SourceID:      "source",
		TransactionID: "tx-retry",
		Offset:        []byte{1},
		Commands:      []CacheCommandRequest{{Command: "SETINT", Key: "retry", Value: "1"}},
	}
	if _, err := coordinator.ApplyBatch(context.Background(), trie, batch); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := coordinator.ApplyBatch(context.Background(), trie, batch)
		if err != nil {
			b.Fatal(err)
		}
		if !result.AlreadyCommitted {
			b.Fatal("retry was not recognized as already committed")
		}
	}
}

func BenchmarkM228BinaryTailEncoding(b *testing.B) {
	tail := CommandJournalTail{
		LastSequence: 1,
		Limit:        1,
		Entries: []CommandJournalRecord{{
			Sequence: 1,
			Request: CacheCommandRequest{
				Command: "BATCH",
				Atomic:  true,
				Batch: []CacheCommandRequest{{
					Command: "SETINT",
					Key:     "orders",
					Value:   "1",
				}},
			},
		}},
	}
	encoded, err := marshalCommandJournalTailBinary(tail)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(encoded)), "tail-bytes/op")
	for index := 0; index < b.N; index++ {
		if _, err := marshalCommandJournalTailBinary(tail); err != nil {
			b.Fatal(err)
		}
	}
}
