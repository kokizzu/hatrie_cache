package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

var ch058BitmapINBenchmarkSink int

func newCH058BitmapINBenchmarkTrie(b *testing.B) *HatTrie {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.Grow(160_000)
	data.WriteByte('[')
	for row := 0; row < 4_000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		fmt.Fprintf(&data, `{"id":%d,"state":"s%d"}`, row, row&7)
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())
	if err := trie.CreateSQLJSONBitmapIndex("events", "state"); err != nil {
		b.Fatal(err)
	}
	return trie
}

func BenchmarkCH058BitmapIndexedINLegacy(b *testing.B) {
	trie := newCH058BitmapINBenchmarkTrie(b)
	values := []interface{}{"s7", "s3", "s1", "s5"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rows := make([]SQLRow, 0, 2_000)
		for _, value := range values {
			candidates, available, err := trie.ResolveSQLIndexedSource("CACHE", "events", "state", value)
			if err != nil || !available {
				b.Fatalf("legacy indexed lookup = %v/%v", available, err)
			}
			rows = append(rows, candidates...)
		}
		ch058BitmapINBenchmarkSink = len(rows)
	}
}

func BenchmarkCH058BitmapIndexedINBatch(b *testing.B) {
	trie := newCH058BitmapINBenchmarkTrie(b)
	values := []interface{}{"s7", "s3", "s1", "s5"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rows, available, err := trie.ResolveSQLIndexedValues("CACHE", "events", "state", values)
		if err != nil || !available {
			b.Fatalf("batch indexed lookup = %v/%v", available, err)
		}
		ch058BitmapINBenchmarkSink = len(rows)
	}
}
