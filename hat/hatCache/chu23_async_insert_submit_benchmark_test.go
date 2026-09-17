package hatCache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkCHU23AsyncInsertSubmitBaseline(b *testing.B) {
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

	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := buffer.Submit(context.Background(), CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("chu23:%d", index),
			Value:   "value",
		}); err != nil {
			b.Fatal(err)
		}
		if (index+1)%64 == 0 {
			if err := buffer.Flush(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	if err := buffer.Flush(context.Background()); err != nil {
		b.Fatal(err)
	}
}
