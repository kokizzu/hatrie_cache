package hatSql

import "testing"

func BenchmarkSQLTextContainsFastPath(b *testing.B) {
	text := "The hat trie cache keeps repeated words close to the query path for fast text filtering."
	query := "hat trie cache"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !textContains(text, query) {
			b.Fatal("text match returned false")
		}
	}
}
