package hatSql_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryDeltaDecodeIntoRoundTripAndReuse(t *testing.T) {
	columns, rows := deltaIntoBenchmarkFixture()
	tests := []struct {
		name   string
		encode func([]hatSql.SQLRowBinaryColumn, []hatSql.SQLRow) ([]byte, error)
	}{
		{name: "delta", encode: hatSql.EncodeSQLRowBinaryDelta},
		{name: "double-delta", encode: hatSql.EncodeSQLRowBinaryDoubleDelta},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := test.encode(columns, rows)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			decoded, err := hatSql.DecodeSQLRowBinaryDeltaInto(nil, columns, encoded)
			if err != nil {
				t.Fatalf("decode into: %v", err)
			}
			if !reflect.DeepEqual(rows, decoded) {
				t.Fatalf("first decode mismatch: got %#v want %#v", decoded, rows)
			}

			reused, err := hatSql.DecodeSQLRowBinaryDeltaInto(decoded[:0], columns, encoded)
			if err != nil {
				t.Fatalf("decode into reuse: %v", err)
			}
			if len(decoded) > 0 && len(reused) > 0 && &decoded[0] != &reused[0] {
				t.Fatal("decode into did not reuse the destination row storage")
			}
			if !reflect.DeepEqual(rows, reused) {
				t.Fatalf("reused decode mismatch: got %#v want %#v", reused, rows)
			}
		})
	}
}
