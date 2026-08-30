package hatCache

import (
	"strings"
	"testing"
)

var benchmarkSQLByteSource string

func BenchmarkSQLJSONByteSource(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.UpsertBytes("events", []byte(`[{"id":1,"payload":"`+strings.Repeat("x", 16<<10)+`"}]`))
	b.ReportAllocs()
	b.Run("clone_and_string_conversion", func(b *testing.B) {
		for iteration := 0; iteration < b.N; iteration++ {
			data, err := trie.GetBytesChecked("events")
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSQLByteSource = string(data)
		}
	})
	b.Run("owned_byte_view", func(b *testing.B) {
		for iteration := 0; iteration < b.N; iteration++ {
			source, err := trie.sqlJSONSource("events")
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSQLByteSource = source.raw
		}
	})
}
