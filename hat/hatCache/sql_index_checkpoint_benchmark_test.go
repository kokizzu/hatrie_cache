package hatCache

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkSQLJSONIndexRebuildCheckpoint(b *testing.B) {
	for _, mode := range []string{"disabled", "memory", "file"} {
		b.Run(mode, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
			if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
				b.Fatal(err)
			}
			if mode != "disabled" {
				var store SQLJSONIndexRebuildCheckpointStore = &sqlIndexRebuildCheckpointTestStore{}
				if mode == "file" {
					fileStore, err := NewFileSQLJSONIndexRebuildCheckpointStore(filepath.Join(b.TempDir(), "checkpoint.json"))
					if err != nil {
						b.Fatal(err)
					}
					store = fileStore
				}
				if err := trie.SetSQLJSONIndexRebuildCheckpointStore(context.Background(), store); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := trie.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
					b.Fatal(err)
				}
				if processed, err := trie.RunScheduledSQLJSONIndexRebuilds(1); err != nil || processed != 1 {
					b.Fatalf("RunScheduledSQLJSONIndexRebuilds() = %d, %v", processed, err)
				}
			}
		})
	}
}
