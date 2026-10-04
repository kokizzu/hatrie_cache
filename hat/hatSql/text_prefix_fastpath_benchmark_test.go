package hatSql

import "testing"

func BenchmarkSQLTextContainsPrefixFastPath(b *testing.B) {
	text := "The hat trie cache keeps repeated words close to the query path for fast text filtering."
	prefix := "repe"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !textContainsPrefix(text, prefix) {
			b.Fatal("text prefix match returned false")
		}
	}
}
