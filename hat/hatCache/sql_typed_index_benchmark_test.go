package hatCache

import "testing"

var sqlTypedIndexBenchmarkSink int

func BenchmarkSQLTypedInt64IndexRebuild(b *testing.B) {
	first := benchmarkSQLIndexSnapshotSource(20_000, "first")
	second := benchmarkSQLIndexSnapshotSource(20_000, "second")
	b.ReportAllocs()
	b.Run("generic_field", func(b *testing.B) {
		index := &sqlJSONFieldIndex{}
		for iteration := 0; iteration < b.N; iteration++ {
			source := first
			if iteration&1 != 0 {
				source = second
			}
			if err := refreshSQLJSONFieldIndexString(index, "events", "id", source); err != nil {
				b.Fatal(err)
			}
		}
		sqlTypedIndexBenchmarkSink = len(index.ordered) + len(index.rows)
	})
	b.Run("typed_int64", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		index := &sqlJSONTypedInt64Index{}
		for iteration := 0; iteration < b.N; iteration++ {
			source := first
			if iteration&1 != 0 {
				source = second
			}
			trie.sqlIndexMu.Lock()
			snapshot, err := trie.sqlJSONIndexSnapshotLocked("events", source)
			if err == nil {
				refreshSQLJSONTypedInt64Index(index, "id", source, snapshot.rows)
			}
			trie.sqlIndexMu.Unlock()
			if err != nil {
				b.Fatal(err)
			}
		}
		sqlTypedIndexBenchmarkSink = len(index.ordered) + len(index.postings)
	})
}
