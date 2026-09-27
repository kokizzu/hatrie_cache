package hatCache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkCH016AsyncInsert(b *testing.B) {
	for _, test := range []struct {
		name   string
		submit func(context.Context, *AsyncInsertBuffer, int) error
	}{
		{
			name: "sql_adapter",
			submit: func(ctx context.Context, buffer *AsyncInsertBuffer, index int) error {
				_, err := buffer.SubmitSQL(ctx, fmt.Sprintf("INSERT INTO cache (key, value) VALUES ('ch016:%d', 'value')", index))
				return err
			},
		},
		{
			name: "manual_compile",
			submit: func(ctx context.Context, buffer *AsyncInsertBuffer, index int) error {
				request, err := CompileSQL(fmt.Sprintf("INSERT INTO cache (key, value) VALUES ('ch016:%d', 'value')", index))
				if err != nil {
					return err
				}
				_, err = buffer.Submit(ctx, request)
				return err
			},
		},
		{
			name: "precompiled_command",
			submit: func(ctx context.Context, buffer *AsyncInsertBuffer, index int) error {
				_, err := buffer.Submit(ctx, CacheCommandRequest{Command: "SETSTR", Key: fmt.Sprintf("ch016:%d", index), Value: "value"})
				return err
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
				GroupCommitMaxBatch: 64,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer journal.Close()
			buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
				BatchSize:     64,
				Capacity:      128,
				FlushInterval: time.Hour,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer buffer.Close(context.Background())

			b.ReportAllocs()
			b.ResetTimer()
			ctx := context.Background()
			for index := 0; index < b.N; index++ {
				if err := test.submit(ctx, buffer, index); err != nil {
					b.Fatal(err)
				}
				if (index+1)%64 == 0 {
					if err := buffer.Flush(ctx); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			if err := buffer.Flush(ctx); err != nil {
				b.Fatal(err)
			}
		})
	}
}
