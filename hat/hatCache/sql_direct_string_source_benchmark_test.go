package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

func sqlResolveSQLColumnarSourceCopied(trie *HatTrie, key string, fields []string) (SQLColumnarBatch, error) {
	data, err := trie.GetBytesChecked(key)
	if err != nil {
		return SQLColumnarBatch{}, err
	}
	return sqlJSONColumnarBatch(key, data, fields)
}

func BenchmarkSQLColumnarStringSource(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	const rows = 20_000
	var data strings.Builder
	data.Grow(rows * 60)
	data.WriteByte('[')
	for index := 0; index < rows; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(index))
		data.WriteString(`,"state":"ready","payload":"cache source benchmark"}`)
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())
	fields := []string{"id", "state"}
	if source, err := trie.sqlJSONSourceString("events"); err != nil {
		b.Fatal(err)
	} else if batch, err := sqlJSONColumnarBatchString("events", source, fields); err != nil || batch.Rows != rows {
		b.Fatalf("columnar warmup = %d/%v", batch.Rows, err)
	}
	if batch, err := sqlResolveSQLColumnarSourceCopied(trie, "events", fields); err != nil || batch.Rows != rows {
		b.Fatalf("copied columnar warmup = %d/%v", batch.Rows, err)
	}
	for _, benchmark := range []struct {
		name string
		run  func() error
	}{
		{"columnar/immutable_source", func() error {
			source, err := trie.sqlJSONSourceString("events")
			if err != nil {
				return err
			}
			_, err = sqlJSONColumnarBatchString("events", source, fields)
			return err
		}},
		{"columnar/copied_source", func() error { _, err := sqlResolveSQLColumnarSourceCopied(trie, "events", fields); return err }},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := benchmark.run(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
