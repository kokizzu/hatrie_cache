package hatSql

import (
	"bytes"
	"reflect"
	"testing"
)

func TestSQLNativeBlockRoundTripPreservesSchemaRowsAndProgress(t *testing.T) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "name", Type: SQLRowBinaryString, Nullable: true},
		{Name: "active", Type: SQLRowBinaryBool},
		{Name: "score", Type: SQLRowBinaryFloat64, Nullable: true},
	}
	rows := []SQLRow{
		{"id": int64(1), "name": "alpha", "active": true, "score": 1.5},
		{"id": int64(2), "name": nil, "active": false, "score": nil},
	}

	encoded, err := EncodeSQLNativeBlock(columns, rows, SQLNativeBlockOptions{
		Sequence: 7,
		Progress: 11,
		Complete: true,
	})
	if err != nil {
		t.Fatalf("EncodeSQLNativeBlock() error = %v", err)
	}
	decoded, err := DecodeSQLNativeBlock(encoded)
	if err != nil {
		t.Fatalf("DecodeSQLNativeBlock() error = %v", err)
	}
	if decoded.Sequence != 7 || decoded.Progress != 11 || !decoded.Complete {
		t.Fatalf("decoded metadata = %#v, want sequence=7 progress=11 complete", decoded)
	}
	if !reflect.DeepEqual(decoded.Columns, columns) {
		t.Fatalf("decoded columns = %#v, want %#v", decoded.Columns, columns)
	}
	if !reflect.DeepEqual(decoded.Rows, rows) {
		t.Fatalf("decoded rows = %#v, want %#v", decoded.Rows, rows)
	}
}

func TestSQLNativeBlockEncodingIsDeterministicAndRejectsTrailingBytes(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}}
	rows := []SQLRow{{"id": int64(1)}, {"id": int64(2)}}
	options := SQLNativeBlockOptions{Sequence: 3, Progress: 4}
	first, err := EncodeSQLNativeBlock(columns, rows, options)
	if err != nil {
		t.Fatalf("first encode error = %v", err)
	}
	second, err := EncodeSQLNativeBlock(columns, rows, options)
	if err != nil {
		t.Fatalf("second encode error = %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("encoding changed between identical inputs: %x != %x", first, second)
	}
	if _, err := DecodeSQLNativeBlock(append(append([]byte(nil), first...), 0)); err == nil {
		t.Fatal("DecodeSQLNativeBlock(trailing bytes) error = nil, want rejection")
	}
}

func TestSQLNativeBlockRejectsInvalidProgressAndSchema(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}}
	if _, err := EncodeSQLNativeBlock(columns, []SQLRow{{"id": int64(1)}}, SQLNativeBlockOptions{Progress: 2, Sequence: 3}); err != nil {
		t.Fatalf("valid progress encode error = %v", err)
	}
	if _, err := EncodeSQLNativeBlock([]SQLRowBinaryColumn{{Name: "", Type: SQLRowBinaryInt64}}, nil, SQLNativeBlockOptions{}); err == nil {
		t.Fatal("empty column name error = nil, want rejection")
	}
}

var nativeBlockBenchmarkSink []byte

func BenchmarkSQLNativeBlockVsRowBinary(b *testing.B) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "region", Type: SQLRowBinaryString, Nullable: true},
		{Name: "amount", Type: SQLRowBinaryFloat64},
	}
	rows := make([]SQLRow, 1024)
	for index := range rows {
		rows[index] = SQLRow{
			"id":     int64(index),
			"region": []string{"apac", "eu", "us"}[index%3],
			"amount": float64(index) * 1.25,
		}
	}
	b.Run("native-block", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			encoded, err := EncodeSQLNativeBlock(columns, rows, SQLNativeBlockOptions{Sequence: 1, Progress: uint64(len(rows))})
			if err != nil {
				b.Fatal(err)
			}
			nativeBlockBenchmarkSink = encoded
		}
		b.ReportMetric(float64(len(nativeBlockBenchmarkSink)), "wire-bytes/op")
	})
	b.Run("row-binary", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			encoded, err := EncodeSQLRowBinary(columns, rows)
			if err != nil {
				b.Fatal(err)
			}
			nativeBlockBenchmarkSink = encoded
		}
		b.ReportMetric(float64(len(nativeBlockBenchmarkSink)), "wire-bytes/op")
	})
}
