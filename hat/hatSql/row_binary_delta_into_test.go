package hatSql_test

import (
	"bytes"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryDeltaEncodeIntoMatchesBothFormats(t *testing.T) {
	columns, rows := deltaIntoBenchmarkFixture()
	testCases := []struct {
		name   string
		encode func([]byte, []hatSql.SQLRowBinaryColumn, []hatSql.SQLRow) ([]byte, error)
		plain  func([]hatSql.SQLRowBinaryColumn, []hatSql.SQLRow) ([]byte, error)
	}{
		{
			name:   "delta",
			encode: hatSql.EncodeSQLRowBinaryDeltaInto,
			plain:  hatSql.EncodeSQLRowBinaryDelta,
		},
		{
			name:   "double_delta",
			encode: hatSql.EncodeSQLRowBinaryDoubleDeltaInto,
			plain:  hatSql.EncodeSQLRowBinaryDoubleDelta,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			expected, err := testCase.plain(columns, rows)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := testCase.encode(nil, columns, rows)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded, expected) {
				t.Fatalf("EncodeInto changed delta wire output")
			}
			reused, err := testCase.encode(encoded[:0], columns, rows)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(reused, expected) {
				t.Fatalf("reused EncodeInto changed delta wire output")
			}
			decoded, err := hatSql.DecodeSQLRowBinaryDelta(columns, reused)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, rows) {
				t.Fatalf("delta EncodeInto round-trip mismatch")
			}
		})
	}
}
