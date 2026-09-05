package hatSql_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryDictionaryRoundTripAcrossBatches(t *testing.T) {
	columns := dictionaryTestColumns()
	dictionaryColumns := []string{"region", "payload", "metadata"}
	encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, dictionaryColumns)
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	decoder, err := hatSql.NewSQLRowBinaryDictionaryDecoder(columns, dictionaryColumns)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	first := []hatSql.SQLRow{
		{"id": int64(1), "region": "sg", "payload": []byte("warm"), "metadata": json.RawMessage(`{"tier":"hot"}`)},
		{"id": int64(2), "region": "jp", "payload": []byte("cold"), "metadata": json.RawMessage(`{"tier":"cold"}`)},
		{"id": int64(3), "region": "sg", "payload": []byte("warm"), "metadata": json.RawMessage(`{"tier":"hot"}`)},
	}
	second := []hatSql.SQLRow{
		{"id": int64(4), "region": "sg", "payload": []byte("warm"), "metadata": json.RawMessage(`{"tier":"hot"}`)},
		{"id": int64(5), "region": "jp", "payload": []byte("cold"), "metadata": json.RawMessage(`{"tier":"cold"}`)},
	}
	firstWire, err := encoder.Encode(first)
	if err != nil {
		t.Fatalf("encode first batch: %v", err)
	}
	gotFirst, err := decoder.Decode(firstWire)
	if err != nil {
		t.Fatalf("decode first batch: %v", err)
	}
	if !reflect.DeepEqual(gotFirst, first) {
		t.Fatalf("first batch mismatch: got %#v want %#v", gotFirst, first)
	}
	secondWire, err := encoder.Encode(second)
	if err != nil {
		t.Fatalf("encode second batch: %v", err)
	}
	gotSecond, err := decoder.Decode(secondWire)
	if err != nil {
		t.Fatalf("decode second batch: %v", err)
	}
	if !reflect.DeepEqual(gotSecond, second) {
		t.Fatalf("second batch mismatch: got %#v want %#v", gotSecond, second)
	}
	plainSecond, err := hatSql.EncodeSQLRowBinary(columns, second)
	if err != nil {
		t.Fatalf("encode plain second batch: %v", err)
	}
	if len(secondWire) >= len(plainSecond) {
		t.Fatalf("dictionary reuse did not reduce repeated batch: dictionary=%d plain=%d", len(secondWire), len(plainSecond))
	}
	decoder.Reset()
	if _, err := decoder.Decode(secondWire); err == nil {
		t.Fatal("decoder accepted a reused batch after reset")
	}
	gotFirst, err = decoder.Decode(firstWire)
	if err != nil {
		t.Fatalf("decode first batch after reset: %v", err)
	}
	if !reflect.DeepEqual(gotFirst, first) {
		t.Fatalf("first batch after reset mismatch: got %#v want %#v", gotFirst, first)
	}
}

func TestSQLRowBinaryDictionaryResetAndFailedEncodeAreAtomic(t *testing.T) {
	columns := dictionaryTestColumns()
	encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	valid := []hatSql.SQLRow{{"id": int64(1), "region": "sg", "payload": []byte("warm"), "metadata": json.RawMessage(`{"tier":"hot"}`)}}
	invalid := []hatSql.SQLRow{{"id": int64(2), "region": 42, "payload": []byte("warm"), "metadata": json.RawMessage(`{"tier":"hot"}`)}}
	if _, err := encoder.Encode(invalid); err == nil {
		t.Fatal("invalid dictionary value unexpectedly encoded")
	}
	firstWire, err := encoder.Encode(valid)
	if err != nil {
		t.Fatalf("encode after failed batch: %v", err)
	}
	encoder.Reset()
	resetWire, err := encoder.Encode(valid)
	if err != nil {
		t.Fatalf("encode after reset: %v", err)
	}
	if !bytes.Equal(firstWire, resetWire) {
		t.Fatalf("reset changed first-batch encoding")
	}
}

func TestSQLRowBinaryDictionaryRejectsMalformedBatchWithoutStateMutation(t *testing.T) {
	columns := dictionaryTestColumns()
	encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	decoder, err := hatSql.NewSQLRowBinaryDictionaryDecoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	rows := []hatSql.SQLRow{{"id": int64(1), "region": "sg", "payload": []byte("warm"), "metadata": json.RawMessage(`{"tier":"hot"}`)}}
	wire, err := encoder.Encode(rows)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := decoder.Decode(wire[:len(wire)-1]); err == nil {
		t.Fatal("truncated dictionary batch unexpectedly decoded")
	}
	got, err := decoder.Decode(wire)
	if err != nil {
		t.Fatalf("decode after malformed batch: %v", err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("decode after malformed batch mismatch: got %#v want %#v", got, rows)
	}
}

func TestSQLRowBinaryDictionaryValidatesSelection(t *testing.T) {
	columns := dictionaryTestColumns()
	for _, selection := range [][]string{
		{"missing"},
		{"region", "region"},
		{"id"},
	} {
		if _, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, selection); err == nil {
			t.Fatalf("selection %#v unexpectedly accepted", selection)
		}
		if _, err := hatSql.NewSQLRowBinaryDictionaryDecoder(columns, selection); err == nil {
			t.Fatalf("decoder selection %#v unexpectedly accepted", selection)
		}
	}
}

func dictionaryTestColumns() []hatSql.SQLRowBinaryColumn {
	return []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "region", Type: hatSql.SQLRowBinaryString},
		{Name: "payload", Type: hatSql.SQLRowBinaryBytes, Nullable: true},
		{Name: "metadata", Type: hatSql.SQLRowBinaryJSON},
	}
}

func BenchmarkSQLRowBinaryDictionaryEncodeFirst(b *testing.B) {
	columns := dictionaryTestColumns()
	rows := dictionaryBenchmarkRows(0, 256)
	encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		encoder.Reset()
		b.StartTimer()
		encoded, err := encoder.Encode(rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
	}
}

func BenchmarkSQLRowBinaryDictionaryEncodeReuse(b *testing.B) {
	columns := dictionaryTestColumns()
	rows := dictionaryBenchmarkRows(0, 256)
	encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := encoder.Encode(rows); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		encoded, err := encoder.Encode(rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
	}
}

func BenchmarkSQLRowBinaryEncodeBaseline(b *testing.B) {
	columns := dictionaryTestColumns()
	rows := dictionaryBenchmarkRows(0, 256)
	b.ReportAllocs()
	for range b.N {
		encoded, err := hatSql.EncodeSQLRowBinary(columns, rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
	}
}

func BenchmarkSQLRowBinaryDictionaryDecodeFirst(b *testing.B) {
	columns := dictionaryTestColumns()
	rows := dictionaryBenchmarkRows(0, 256)
	encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := encoder.Encode(rows)
	if err != nil {
		b.Fatal(err)
	}
	decoder, err := hatSql.NewSQLRowBinaryDictionaryDecoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(encoded)), "wire-B")
	for range b.N {
		b.StopTimer()
		decoder.Reset()
		b.StartTimer()
		if _, err := decoder.Decode(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLRowBinaryDictionaryDecodeReuse(b *testing.B) {
	columns := dictionaryTestColumns()
	rows := dictionaryBenchmarkRows(0, 256)
	encoder, err := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := encoder.Encode(rows); err != nil {
		b.Fatal(err)
	}
	encoded, err := encoder.Encode(rows)
	if err != nil {
		b.Fatal(err)
	}
	decoder, err := hatSql.NewSQLRowBinaryDictionaryDecoder(columns, []string{"region", "payload", "metadata"})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := decoder.Decode(func() []byte {
		firstEncoder, _ := hatSql.NewSQLRowBinaryDictionaryEncoder(columns, []string{"region", "payload", "metadata"})
		first, _ := firstEncoder.Encode(rows)
		return first
	}()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(encoded)), "wire-B")
	for range b.N {
		if _, err := decoder.Decode(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLRowBinaryDecodeBaseline(b *testing.B) {
	columns := dictionaryTestColumns()
	rows := dictionaryBenchmarkRows(0, 256)
	encoded, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(encoded)), "wire-B")
	for range b.N {
		if _, err := hatSql.DecodeSQLRowBinary(columns, encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func dictionaryBenchmarkRows(start, count int) []hatSql.SQLRow {
	rows := make([]hatSql.SQLRow, count)
	regions := []string{"sg", "jp", "us", "de"}
	payloads := [][]byte{[]byte("warm"), []byte("cold"), []byte("queued"), []byte("done")}
	metadata := []json.RawMessage{
		json.RawMessage(`{"tier":"hot"}`),
		json.RawMessage(`{"tier":"cold"}`),
		json.RawMessage(`{"tier":"warm"}`),
		json.RawMessage(`{"tier":"cold"}`),
	}
	for index := range rows {
		value := index % len(regions)
		rows[index] = hatSql.SQLRow{
			"id":       int64(start + index),
			"region":   regions[value],
			"payload":  payloads[value],
			"metadata": metadata[value],
		}
	}
	return rows
}
