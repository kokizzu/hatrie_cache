package hatSql_test

import (
	"bytes"
	"reflect"
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

func TestSQLRowBinaryDictionaryDecodeIntoReusesRowsAndByteValues(t *testing.T) {
	columns := dictionaryTestColumns()
	rows := dictionaryBenchmarkRows(0, 256)
	encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := encoder.Encode(rows)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encoder.Encode(rows)
	if err != nil {
		t.Fatal(err)
	}

	decoder, err := hatSql.NewSQLRowBinaryDictionaryDecoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decoder.DecodeInto(nil, first)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatalf("first DecodeInto result differs from input")
	}
	firstRow := &decoded[0]
	firstPayload := decoded[0]["payload"].([]byte)
	firstMetadata := reflect.ValueOf(decoded[0]["metadata"]).Bytes()

	reused, err := decoder.DecodeInto(decoded[:0], second)
	if err != nil {
		t.Fatal(err)
	}
	if &reused[0] != firstRow {
		t.Fatalf("DecodeInto did not reuse the row slice")
	}
	if !reflect.DeepEqual(reused, rows) {
		t.Fatalf("reused DecodeInto result differs from input")
	}
	reusedPayload := reused[0]["payload"].([]byte)
	if &reusedPayload[0] != &firstPayload[0] {
		t.Fatalf("DecodeInto did not reuse the byte value buffer")
	}
	reusedMetadata := reflect.ValueOf(reused[0]["metadata"]).Bytes()
	if &reusedMetadata[0] != &firstMetadata[0] {
		t.Fatalf("DecodeInto did not reuse the JSON value buffer")
	}
}
