package hatCache

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

var benchmarkCHU23AsyncInsertQueueStatsSink AsyncInsertQueuesResponse

func BenchmarkCHU23AsyncInsertQueueStats(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journ, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journ.Close()
	buffer, err := NewAsyncInsertBuffer(journ, trie, AsyncInsertBufferOptions{
		BatchSize:     64,
		Capacity:      128,
		FlushInterval: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer buffer.Close(context.Background())
	registry, err := NewAsyncInsertQueueRegistry(1)
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register("writer", buffer); err != nil {
		b.Fatal(err)
	}
	if _, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "SETSTR", Key: "chu23:status", Value: "value"}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		benchmarkCHU23AsyncInsertQueueStatsSink = AsyncInsertQueuesResponse{Queues: registry.Stats()}
	}
	b.StopTimer()
}
