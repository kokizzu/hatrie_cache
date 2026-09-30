package hatSql_test

import (
	"bytes"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryAdaptiveEncoderReusesStateAndPreservesWire(t *testing.T) {
	columns, rows := rowBinaryAdaptiveEncoderFixture()
	want, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryAdaptive() error = %v", err)
	}

	var encoder hatSql.SQLRowBinaryAdaptiveEncoder
	dst := make([]byte, 0, len(want)+16)
	got, err := encoder.EncodeInto(dst, columns, rows)
	if err != nil {
		t.Fatalf("AdaptiveEncoder.EncodeInto() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("AdaptiveEncoder.EncodeInto() = %x, want %x", got, want)
	}

	rows[0]["id"] = int64(1 << 40)
	want, err = hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		t.Fatalf("changed EncodeSQLRowBinaryAdaptive() error = %v", err)
	}
	got, err = encoder.EncodeInto(got, columns, rows)
	if err != nil {
		t.Fatalf("changed AdaptiveEncoder.EncodeInto() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("changed AdaptiveEncoder.EncodeInto() = %x, want %x", got, want)
	}

	emptyDst := make([]byte, 2, 16)
	emptyBacking := &emptyDst[:cap(emptyDst)][0]
	empty, err := encoder.EncodeInto(emptyDst, columns, nil)
	if err != nil {
		t.Fatalf("empty AdaptiveEncoder.EncodeInto() error = %v", err)
	}
	if empty == nil || len(empty) != 0 || &empty[:cap(empty)][0] != emptyBacking {
		t.Fatalf("empty AdaptiveEncoder.EncodeInto() = len %d cap %d, want reused empty buffer", len(empty), cap(empty))
	}

	encoder.Reset()
	got, err = encoder.EncodeInto(nil, columns, rows)
	if err != nil {
		t.Fatalf("reset AdaptiveEncoder.EncodeInto() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("reset AdaptiveEncoder.EncodeInto() = %x, want %x", got, want)
	}

	if _, err := encoder.EncodeInto(got, columns, []hatSql.SQLRow{{"id": nil, "at": rows[0]["at"], "label": "bad"}}); err == nil {
		t.Fatal("invalid non-nullable row was accepted")
	}
	if _, err := encoder.EncodeInto(got, columns, rows); err != nil {
		t.Fatalf("encoder unusable after rejected input: %v", err)
	}
}

func rowBinaryAdaptiveEncoderFixture() ([]hatSql.SQLRowBinaryColumn, []hatSql.SQLRow) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
		{Name: "label", Type: hatSql.SQLRowBinaryString},
	}
	rows := make([]hatSql.SQLRow, 128)
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":    int64(index + 1),
			"at":    start.Add(time.Duration(index) * time.Second),
			"label": "steady",
		}
	}
	return columns, rows
}
