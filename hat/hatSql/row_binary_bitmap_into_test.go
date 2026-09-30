package hatSql_test

import (
	"bytes"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryBitmapEncodeIntoMatchesEncode(t *testing.T) {
	columns, rows := bitmapIntoBenchmarkFixture()
	expected, err := hatSql.EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := hatSql.EncodeSQLRowBinaryBitmapInto(nil, columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, expected) {
		t.Fatalf("EncodeInto changed nullable bitmap wire output")
	}

	reused, err := hatSql.EncodeSQLRowBinaryBitmapInto(encoded[:0], columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reused, expected) {
		t.Fatalf("reused EncodeInto changed nullable bitmap wire output")
	}
	decoded, err := hatSql.DecodeSQLRowBinaryBitmap(columns, reused)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatalf("EncodeInto round-trip mismatch")
	}
}
