package hatCache

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkCHU37SystemMutationsLegacy(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	const entries = 256
	for index := 0; index < entries; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("key:%d", index),
			Value:   "value",
		})
		if !response.OK {
			b.Fatalf("seed %d failed: %#v", index, response)
		}
	}
	resolver := NewSQLSystemTablesResolver(trie, SQLSystemTablesResolverOptions{
		Journal:       journal,
		MutationLimit: entries,
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := resolver.ResolveSQLSource("CACHE", SQLSystemMutationsTable)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != entries {
			b.Fatalf("rows = %d, want %d", len(rows), entries)
		}
	}
}

func BenchmarkCHU37SystemMutationsProvider(b *testing.B) {
	mutations := make([]SQLSystemMutation, 256)
	for index := range mutations {
		mutations[index] = SQLSystemMutation{
			MutationID:    fmt.Sprintf("mut-%d", index),
			Sequence:      int64(index),
			Command:       "MERGE",
			Key:           "orders",
			State:         "committed",
			Progress:      100,
			AffectedParts: []string{"part-a", "part-b"},
			ErrorCode:     "",
			ErrorMessage:  "",
		}
	}
	resolver := NewSQLSystemTablesResolver(nil, SQLSystemTablesResolverOptions{
		MutationProvider: SQLSystemMutationProviderFunc(func() ([]SQLSystemMutation, error) {
			return mutations, nil
		}),
		MutationLimit: len(mutations),
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := resolver.ResolveSQLSource("CACHE", SQLSystemMutationsTable)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != len(mutations) {
			b.Fatalf("rows = %d, want %d", len(rows), len(mutations))
		}
	}
}
