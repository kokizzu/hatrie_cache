package hatCache

import (
	"context"
	"testing"
)

var sqlJSONIndexRebuildProgressBenchmarkSink SQLJSONIndexRebuildProgress

func BenchmarkSQLJSONIndexRebuildProgress(b *testing.B) {
	for _, benchmark := range []struct {
		name         string
		withProgress bool
	}{
		{name: "legacy"},
		{name: "progress", withProgress: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			trie := CreateHatTrie()
			trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
			if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
				b.Fatal(err)
			}
			for b.Loop() {
				trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
				if err := trie.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
					b.Fatal(err)
				}
				if benchmark.withProgress {
					if _, err := trie.RunScheduledSQLJSONIndexRebuildsWithProgress(context.Background(), 1, func(progress SQLJSONIndexRebuildProgress) {
						sqlJSONIndexRebuildProgressBenchmarkSink = progress
					}); err != nil {
						b.Fatal(err)
					}
				} else if _, err := trie.RunScheduledSQLJSONIndexRebuilds(1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
