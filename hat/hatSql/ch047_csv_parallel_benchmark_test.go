package hatSql

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strings"
	"testing"
)

func BenchmarkCH047CSVSerialImport(b *testing.B) {
	data := ch047CSVParallelBenchmarkData()
	options := ExternalImportOptions{MaxRows: 20_000, MaxBytes: int64(len(data) + 1)}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tables := NewExternalTables()
		if err := tables.ImportCSVReader("events", bytes.NewReader(data), options); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH047CSVParallelImport(b *testing.B) {
	data := ch047CSVParallelBenchmarkData()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tables := NewExternalTables()
		if err := tables.ImportCSVParallel("events", data, 4); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH047CSVSerialParse(b *testing.B) {
	data := ch047CSVParallelBenchmarkData()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ch047CSVSerialParse(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH047CSVParallelParse(b *testing.B) {
	data := ch047CSVParallelBenchmarkData()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ParseCSVParallel(bytes.NewReader(data), 4); err != nil {
			b.Fatal(err)
		}
	}
}

func ch047CSVSerialParse(data []byte) ([]Row, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("CSV requires a header row")
	}
	columns, err := externalTableColumns(records[0])
	if err != nil {
		return nil, err
	}
	rows := make([]Row, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) != len(columns) {
			return nil, fmt.Errorf("CSV field count mismatch")
		}
		row := make(Row, len(columns))
		for column, value := range record {
			row[columns[column]] = value
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func ch047CSVParallelBenchmarkData() []byte {
	var builder strings.Builder
	builder.Grow(700_000)
	builder.WriteString("id,name,note\n")
	for index := 0; index < 20_000; index++ {
		if index%97 == 0 {
			fmt.Fprintf(&builder, "%d,name-%d,\"line-%d\ncontinued\"\n", index, index, index)
			continue
		}
		fmt.Fprintf(&builder, "%d,name-%d,note-%d\n", index, index, index)
	}
	return []byte(builder.String())
}
