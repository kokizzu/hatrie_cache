package hatSql

import (
	"bytes"
	"testing"
)

func BenchmarkExternalTablesImportCSVReader(b *testing.B) {
	data := chU21BenchmarkCSVData()
	tables := NewExternalTables()
	options := ExternalImportOptions{MaxRows: 20_000, MaxBytes: int64(len(data) + 1)}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := tables.ImportCSVReader("people", bytes.NewReader(data), options); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExternalTablesImportJSONEachRowReader(b *testing.B) {
	data := chU21BenchmarkJSONEachRowData()
	tables := NewExternalTables()
	options := ExternalImportOptions{MaxRows: 20_000, MaxBytes: int64(len(data) + 1), MaxRecordBytes: 1024}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := tables.ImportJSONEachRowReader("people", bytes.NewReader(data), options); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStreamCSV(b *testing.B) {
	data := chU21BenchmarkCSVData()
	options := ExternalImportOptions{MaxRows: 20_000, MaxBytes: int64(len(data) + 1)}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows := 0
		err := StreamCSV(bytes.NewReader(data), options, func(_ []string, _ []string) error {
			rows++
			return nil
		})
		if err != nil || rows != 20_000 {
			b.Fatalf("StreamCSV() rows=%d error=%v", rows, err)
		}
	}
}

func BenchmarkStreamJSONEachRow(b *testing.B) {
	data := chU21BenchmarkJSONEachRowData()
	options := ExternalImportOptions{MaxRows: 20_000, MaxBytes: int64(len(data) + 1), MaxRecordBytes: 1024}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows := 0
		err := StreamJSONEachRow(bytes.NewReader(data), options, func(Row) error {
			rows++
			return nil
		})
		if err != nil || rows != 20_000 {
			b.Fatalf("StreamJSONEachRow() rows=%d error=%v", rows, err)
		}
	}
}
