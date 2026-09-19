package hatCache

import (
	"strconv"
	"testing"
)

func BenchmarkCHU36SystemPartsLegacy(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if err := trie.ConfigureLocalPartitions(64); err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 4096; index++ {
		response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: strconv.Itoa(index), Value: "value"})
		if !response.OK {
			b.Fatalf("seed %d failed: %#v", index, response)
		}
	}
	resolver := NewSQLSystemTablesResolver(trie, SQLSystemTablesResolverOptions{})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := resolver.ResolveSQLSource("CACHE", SQLSystemPartsTable)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != 64 {
			b.Fatalf("rows = %d, want 64", len(rows))
		}
	}
}

func BenchmarkCHU36SystemPartsProvider(b *testing.B) {
	parts := make([]SQLSystemPart, 64)
	for index := range parts {
		parts[index] = SQLSystemPart{
			Name:        "part-" + strconv.Itoa(index),
			Partition:   int64(index),
			Rows:        64,
			BytesOnDisk: 4096,
			Active:      true,
			State:       "active",
			Level:       1,
			DataVersion: 1,
			Checksum:    "sha256:fixture",
		}
	}
	resolver := NewSQLSystemTablesResolver(nil, SQLSystemTablesResolverOptions{
		PartProvider: SQLSystemPartProviderFunc(func() ([]SQLSystemPart, error) {
			return parts, nil
		}),
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := resolver.ResolveSQLSource("CACHE", SQLSystemPartsTable)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != len(parts) {
			b.Fatalf("rows = %d, want %d", len(rows), len(parts))
		}
	}
}
