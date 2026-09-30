package hatSql_test

import (
	"bytes"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryAdaptiveIntoReusesDestinationAndPreservesWire(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "label", Type: hatSql.SQLRowBinaryString},
	}
	rows := []hatSql.SQLRow{
		{"id": int64(1), "label": "steady"},
		{"id": int64(2), "label": "steady"},
		{"id": int64(3), "label": "steady"},
	}
	want, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryAdaptive() error = %v", err)
	}
	dst := make([]byte, len(want), len(want)+32)
	for index := range dst {
		dst[index] = 0xa5
	}
	backing := &dst[:cap(dst)][0]
	got, err := hatSql.EncodeSQLRowBinaryAdaptiveInto(dst, columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryAdaptiveInto() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeSQLRowBinaryAdaptiveInto() = %x, want %x", got, want)
	}
	if &got[0] != backing {
		t.Fatal("EncodeSQLRowBinaryAdaptiveInto() did not reuse destination backing storage")
	}

	emptyDst := make([]byte, 3, 16)
	emptyBacking := &emptyDst[:cap(emptyDst)][0]
	empty, err := hatSql.EncodeSQLRowBinaryAdaptiveInto(emptyDst, columns, nil)
	if err != nil {
		t.Fatalf("empty EncodeSQLRowBinaryAdaptiveInto() error = %v", err)
	}
	if empty == nil || len(empty) != 0 || &empty[:cap(empty)][0] != emptyBacking {
		t.Fatalf("empty EncodeSQLRowBinaryAdaptiveInto() = len %d cap %d, want reused empty buffer", len(empty), cap(empty))
	}

	if _, err := hatSql.EncodeSQLRowBinaryAdaptiveInto(dst, columns, []hatSql.SQLRow{{"id": nil, "label": "bad"}}); err == nil {
		t.Fatal("invalid non-nullable row was accepted")
	}
}
