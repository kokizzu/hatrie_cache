package hatCache

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func newCHU01BenchmarkBuffer(b *testing.B, idempotency bool) (*AsyncInsertBuffer, *CommandJournal, *HatTrie, []CacheCommandRequest) {
	b.Helper()
	trie := CreateHatTrie()
	options := CommandJournalOptions{GroupCommitMaxBatch: 64}
	if idempotency {
		options.IdempotencyCapacity = 128
	}
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), options)
	if err != nil {
		trie.Destroy()
		b.Fatal(err)
	}
	journal.syncHook = func() error { return nil }
	buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
		BatchSize:     64,
		Capacity:      128,
		FlushInterval: time.Hour,
	})
	if err != nil {
		_ = journal.Close()
		trie.Destroy()
		b.Fatal(err)
	}
	requests := make([]CacheCommandRequest, 64)
	for index := range requests {
		request := CacheCommandRequest{
			Command: "SET",
			Key:     "chu01:benchmark:" + strconv.Itoa(index),
			Value:   "value",
		}
		if idempotency {
			request.IdempotencyKey = "chu01:insert:" + strconv.Itoa(index)
		}
		requests[index] = request
	}
	return buffer, journal, trie, requests
}

func BenchmarkCHU01AsyncInsertUnkeyed(b *testing.B) {
	buffer, journal, trie, requests := newCHU01BenchmarkBuffer(b, false)
	defer func() {
		_ = buffer.Close(context.Background())
		_ = journal.Close()
		trie.Destroy()
	}()

	b.ReportAllocs()
	b.SetBytes(int64(len(requests)))
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for _, request := range requests {
			if _, err := buffer.Submit(context.Background(), request); err != nil {
				b.Fatal(err)
			}
		}
		if err := buffer.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU01AsyncInsertKeyedDuplicate(b *testing.B) {
	buffer, journal, trie, requests := newCHU01BenchmarkBuffer(b, true)
	defer func() {
		_ = buffer.Close(context.Background())
		_ = journal.Close()
		trie.Destroy()
	}()

	b.ReportAllocs()
	b.SetBytes(int64(len(requests)))
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for _, request := range requests {
			if _, err := buffer.Submit(context.Background(), request); err != nil {
				b.Fatal(err)
			}
		}
		if err := buffer.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
