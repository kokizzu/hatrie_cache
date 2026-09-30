package hatSql_test

import (
	"bytes"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryDictionaryEncodeIntoMatchesEncodeAcrossBatches(t *testing.T) {
	columns := dictionaryTestColumns()
	rows := dictionaryBenchmarkRows(0, 256)

	intoEncoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := intoEncoder.EncodeInto(nil, rows)
	if err != nil {
		t.Fatal(err)
	}
	firstSnapshot := append([]byte(nil), first...)
	second, err := intoEncoder.EncodeInto(first[:0], rows)
	if err != nil {
		t.Fatal(err)
	}

	expectedEncoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		t.Fatal(err)
	}
	expectedFirst, err := expectedEncoder.Encode(rows)
	if err != nil {
		t.Fatal(err)
	}
	expectedSecond, err := expectedEncoder.Encode(rows)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstSnapshot, expectedFirst) || !bytes.Equal(second, expectedSecond) {
		t.Fatalf("EncodeInto changed dictionary wire output")
	}

	decoder, err := hatSql.NewSQLRowBinaryDictionaryDecoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decoder.Decode(firstSnapshot); err != nil {
		t.Fatal(err)
	}
	decoded, err := decoder.Decode(second)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(rows) {
		t.Fatalf("decoded rows = %d, want %d", len(decoded), len(rows))
	}
}
