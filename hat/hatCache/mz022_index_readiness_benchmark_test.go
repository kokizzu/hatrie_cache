package hatCache

import (
	"context"
	"testing"
)

func BenchmarkMZ022IndexMaintenanceStatsCurrent(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		b.Fatal(err)
	}
	if _, _, err := trie.SQLJSONIndexHealth("jobs", "state"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := trie.SQLJSONIndexMaintenanceStats("jobs", "state"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ022WaitSQLJSONIndexReadyCurrent(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		b.Fatal(err)
	}
	if _, _, err := trie.SQLJSONIndexHealth("jobs", "state"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := trie.WaitSQLJSONIndexReady(context.Background(), "jobs", "state"); err != nil {
			b.Fatal(err)
		}
	}
}
