package hatCache

import "testing"

var ch059BitmapEqualityBenchmarkSink int

func BenchmarkCH059BitmapIndexedEquality(b *testing.B) {
	trie := newCH058BitmapINBenchmarkTrie(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rows, available, err := trie.ResolveSQLIndexedSource("CACHE", "events", "state", "s3")
		if err != nil || !available {
			b.Fatalf("bitmap equality lookup = %v/%v", available, err)
		}
		ch059BitmapEqualityBenchmarkSink = len(rows)
	}
}
