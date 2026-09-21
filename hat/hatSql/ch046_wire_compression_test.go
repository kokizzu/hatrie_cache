package hatSql

import (
	"bytes"
	"reflect"
	"testing"
)

func TestCH046ColumnarWireCompressionRoundTripsAndSupportsLegacyFallback(t *testing.T) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "state", Type: SQLRowBinaryString},
	}
	rows := make([]Row, 256)
	for index := range rows {
		rows[index] = Row{"id": int64(index), "state": "ready"}
	}

	var compressed bytes.Buffer
	writer := NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(
		&compressed,
		columns,
		64,
		SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionAuto},
	)
	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("compressed WriteRow() error = %v", err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("compressed Finish() error = %v", err)
	}
	if got := roundTripCH046Rows(t, compressed.Bytes()); !reflect.DeepEqual(got, rows) {
		t.Fatalf("compressed round trip = %#v, want %#v", got, rows)
	}

	var legacy bytes.Buffer
	legacyWriter := NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(
		&legacy,
		columns,
		64,
		SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionNone},
	)
	for _, row := range rows {
		if err := legacyWriter.WriteRow(row); err != nil {
			t.Fatalf("legacy WriteRow() error = %v", err)
		}
	}
	if err := legacyWriter.Finish(); err != nil {
		t.Fatalf("legacy Finish() error = %v", err)
	}
	if got := roundTripCH046Rows(t, legacy.Bytes()); !reflect.DeepEqual(got, rows) {
		t.Fatalf("legacy round trip = %#v, want %#v", got, rows)
	}
	if len(compressed.Bytes()) >= len(legacy.Bytes()) {
		t.Fatalf("adaptive wire bytes = %d, legacy bytes = %d; compression did not reduce payload", len(compressed.Bytes()), len(legacy.Bytes()))
	}
}

func TestCH046ColumnarWireCompressionRejectsInvalidOptionsAndCorruption(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}}
	var encoded bytes.Buffer
	writer := NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(
		&encoded,
		columns,
		2,
		SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionFlate, CompressionLevel: 99},
	)
	if err := writer.WriteRow(Row{"id": int64(1)}); err == nil {
		t.Fatal("invalid compression level unexpectedly succeeded")
	}

	encoded.Reset()
	writer = NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(
		&encoded,
		columns,
		2,
		SQLColumnarBlockStreamOptions{Compression: SQLColumnarBlockStreamCompressionFlate},
	)
	if err := writer.WriteRow(Row{"id": int64(1)}); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	corrupt := append([]byte(nil), encoded.Bytes()...)
	corrupt[len(corrupt)-1] ^= 0xff
	reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(corrupt))
	if err != nil {
		t.Fatalf("reader creation error = %v", err)
	}
	for reader.NextBlock() {
	}
	if reader.Err() == nil {
		t.Fatal("corrupted compressed stream was accepted")
	}
}

func roundTripCH046Rows(t *testing.T, wire []byte) []Row {
	t.Helper()
	reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(wire))
	if err != nil {
		t.Fatalf("reader creation error = %v", err)
	}
	var rows []Row
	for reader.NextBlock() {
		rows = append(rows, reader.Block()...)
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader Err() = %v", err)
	}
	return rows
}
