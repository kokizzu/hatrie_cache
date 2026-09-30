package hatSql_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryBitmapDecodeIntoReusesRowsAndByteValues(t *testing.T) {
	columns, rows := bitmapIntoBenchmarkFixture()
	encoded, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := hatSql.DecodeSQLRowBinaryBitmapInto(nil, columns, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatalf("first DecodeInto result differs from input")
	}
	firstRow := &decoded[1]
	firstPayload := decoded[1]["payload"].([]byte)

	reused, err := hatSql.DecodeSQLRowBinaryBitmapInto(decoded[:0], columns, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if &reused[1] != firstRow {
		t.Fatalf("DecodeInto did not reuse the row slice")
	}
	if !reflect.DeepEqual(reused, rows) {
		t.Fatalf("reused DecodeInto result differs from input")
	}
	reusedPayload := reused[1]["payload"].([]byte)
	if &reusedPayload[0] != &firstPayload[0] {
		t.Fatalf("DecodeInto did not reuse the byte value buffer")
	}
}
