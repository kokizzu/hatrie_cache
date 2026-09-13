package hatCache

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func BenchmarkCH009AsyncInsertBuffer(b *testing.B) {
	b.Run("submit", func(b *testing.B) {
		trie := CreateHatTrie()
		journalPath := filepath.Join(b.TempDir(), "commands.journal")
		journal, err := OpenCommandJournalWithOptions(journalPath, CommandJournalOptions{
			GroupCommitMaxBatch: 64,
		})
		if err != nil {
			trie.Destroy()
			b.Fatal(err)
		}
		buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
			BatchSize:     64,
			Capacity:      4096,
			FlushInterval: time.Hour,
		})
		if err != nil {
			_ = journal.Close()
			trie.Destroy()
			b.Fatal(err)
		}
		request := CacheCommandRequest{Command: "SETSTR", Key: "benchmark:buffer", Value: "value"}

		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := buffer.Submit(nil, request); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := buffer.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
		if err := journal.Close(); err != nil {
			b.Fatal(err)
		}
		trie.Destroy()
	})

	b.Run("submit_flush_64", func(b *testing.B) {
		trie := CreateHatTrie()
		journalPath := filepath.Join(b.TempDir(), "commands.journal")
		journal, err := OpenCommandJournalWithOptions(journalPath, CommandJournalOptions{
			GroupCommitMaxBatch: 64,
		})
		if err != nil {
			trie.Destroy()
			b.Fatal(err)
		}
		buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
			BatchSize:     64,
			Capacity:      64,
			FlushInterval: time.Hour,
		})
		if err != nil {
			_ = journal.Close()
			trie.Destroy()
			b.Fatal(err)
		}
		requests := make([]CacheCommandRequest, 64)
		for index := range requests {
			requests[index] = CacheCommandRequest{
				Command: "SETSTR",
				Key:     "benchmark:buffer:" + strconv.Itoa(index),
				Value:   "value",
			}
		}

		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			for _, request := range requests {
				if _, err := buffer.Submit(nil, request); err != nil {
					b.Fatal(err)
				}
			}
			if err := buffer.Flush(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(64, "writes/op")
		if err := buffer.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
		if err := journal.Close(); err != nil {
			b.Fatal(err)
		}
		trie.Destroy()
	})
}
