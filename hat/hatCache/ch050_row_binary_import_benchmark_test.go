package hatCache

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"

	json "github.com/goccy/go-json"
)

func ch050BenchmarkRows() []hatSql.SQLRow {
	rows := make([]hatSql.SQLRow, 10000)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"key":   fmt.Sprintf("benchmark:%05d", index),
			"value": fmt.Sprintf("value-%05d", index),
		}
	}
	return rows
}

func ch050BenchmarkJSONPayload(b *testing.B) []byte {
	b.Helper()
	payload, err := json.Marshal(ch050BenchmarkRows())
	if err != nil {
		b.Fatal(err)
	}
	return payload
}

func ch050BenchmarkRowBinaryPayload(b *testing.B) []byte {
	b.Helper()
	var encoded bytes.Buffer
	writer, err := hatSql.NewSQLRowBinaryStreamWriterWithColumns(&encoded, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString},
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, row := range ch050BenchmarkRows() {
		if err := writer.WriteRow(row); err != nil {
			b.Fatal(err)
		}
	}
	if err := writer.Finish(); err != nil {
		b.Fatal(err)
	}
	return encoded.Bytes()
}

func BenchmarkCH050JSONRowsDecode(b *testing.B) {
	payload := ch050BenchmarkJSONPayload(b)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		var rows []hatSql.SQLRow
		if err := json.Unmarshal(payload, &rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH050RowBinaryStreamDecode(b *testing.B) {
	payload := ch050BenchmarkRowBinaryPayload(b)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		reader, err := hatSql.NewSQLRowBinaryStreamReader(bytes.NewReader(payload))
		if err != nil {
			b.Fatal(err)
		}
		rows := 0
		for reader.Next() {
			rows++
		}
		if err := reader.Err(); err != nil {
			b.Fatal(err)
		}
		if rows != 10000 {
			b.Fatalf("decoded %d rows, want 10000", rows)
		}
	}
}

func BenchmarkCH050RowBinaryStreamMaterializedDecode(b *testing.B) {
	payload := ch050BenchmarkRowBinaryPayload(b)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		_, rows, err := hatSql.DecodeSQLRowBinaryStream(payload)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != 10000 {
			b.Fatalf("decoded %d rows, want 10000", len(rows))
		}
	}
}

func BenchmarkCH050RowBinaryImport(b *testing.B) {
	payload := ch050BenchmarkRowBinaryPayload(b)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		trie := CreateHatTrie()
		result, err := ExecuteSQLRowBinaryInsert(context.Background(), trie, "INSERT INTO CACHE(key, value)", bytes.NewReader(payload), SQLRowBinaryImportOptions{})
		trie.Destroy()
		if err != nil {
			b.Fatal(err)
		}
		if result.Affected != 10000 {
			b.Fatalf("imported %d rows, want 10000", result.Affected)
		}
	}
}
