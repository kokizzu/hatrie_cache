package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

var sqlIndexSnapshotBenchmarkSink int

func benchmarkSQLIndexSnapshotSource(rows int, suffix string) string {
	var source strings.Builder
	source.Grow(rows * 72)
	source.WriteByte('[')
	for row := 0; row < rows; row++ {
		if row > 0 {
			source.WriteByte(',')
		}
		source.WriteString(`{"id":`)
		source.WriteString(strconv.Itoa(row))
		source.WriteString(`,"state":"ready","name":"event `)
		source.WriteString(strconv.Itoa(row))
		source.WriteByte(' ')
		source.WriteString(suffix)
		source.WriteString(`"}`)
	}
	source.WriteByte(']')
	return source.String()
}

func BenchmarkSQLJSONIndexSnapshotRebuild(b *testing.B) {
	const rows = 20_000
	first := benchmarkSQLIndexSnapshotSource(rows, "first")
	second := benchmarkSQLIndexSnapshotSource(rows, "second")
	b.ReportAllocs()
	b.Run("independent_decodes", func(b *testing.B) {
		field := &sqlJSONFieldIndex{}
		bitmap := &sqlJSONBitmapIndex{}
		text := &sqlJSONTextIndex{}
		composite := &sqlJSONCompositeIndex{fields: []string{"id", "state"}}
		for iteration := 0; iteration < b.N; iteration++ {
			source := first
			if iteration&1 != 0 {
				source = second
			}
			if err := refreshSQLJSONFieldIndexString(field, "events", "id", source); err != nil {
				b.Fatal(err)
			}
			if err := refreshSQLJSONBitmapIndexString(bitmap, "events", "state", source); err != nil {
				b.Fatal(err)
			}
			if err := refreshSQLJSONTextIndexString(text, "events", "name", source); err != nil {
				b.Fatal(err)
			}
			if err := refreshSQLJSONCompositeIndexString(composite, "events", source); err != nil {
				b.Fatal(err)
			}
		}
		sqlIndexSnapshotBenchmarkSink = len(field.ordered) + len(bitmap.rows) + len(text.rows) + len(composite.rows)
	})
	b.Run("shared_snapshot", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		field := &sqlJSONFieldIndex{}
		bitmap := &sqlJSONBitmapIndex{}
		text := &sqlJSONTextIndex{}
		composite := &sqlJSONCompositeIndex{fields: []string{"id", "state"}}
		for iteration := 0; iteration < b.N; iteration++ {
			source := first
			if iteration&1 != 0 {
				source = second
			}
			trie.sqlIndexMu.Lock()
			snapshot, err := trie.sqlJSONIndexSnapshotLocked("events", source)
			if err == nil {
				err = refreshSQLJSONFieldIndexRows(field, "id", source, snapshot.rows)
			}
			if err == nil {
				err = refreshSQLJSONBitmapIndexRows(bitmap, "state", source, snapshot.rows)
			}
			if err == nil {
				err = refreshSQLJSONTextIndexRows(text, "name", source, snapshot.rows)
			}
			if err == nil {
				err = refreshSQLJSONCompositeIndexRows(composite, source, snapshot.rows)
			}
			trie.sqlIndexMu.Unlock()
			if err != nil {
				b.Fatal(err)
			}
		}
		sqlIndexSnapshotBenchmarkSink = len(field.ordered) + len(bitmap.rows) + len(text.rows) + len(composite.rows)
	})
}
