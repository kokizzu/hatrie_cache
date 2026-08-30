package hatCache

import (
	"strings"
	"testing"
)

func BenchmarkSQLJSONIndexAdmissionDenied(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	data := `[{"id":1,"payload":"` + strings.Repeat("x", 8<<20) + `"}]`
	trie.UpsertString("events", data)
	if err := trie.SetSQLJSONIndexAdmissionBudget(SQLJSONIndexAdmissionBudget{MaxSourceBytes: 1}); err != nil {
		b.Fatal(err)
	}
	if err := trie.CreateSQLJSONFieldIndex("events", "id"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_, available, err := trie.ResolveSQLIndexedSource("CACHE", "events", "id", float64(1))
		if err != nil || available {
			b.Fatalf("ResolveSQLIndexedSource() = available %v, error %v", available, err)
		}
	}
}
