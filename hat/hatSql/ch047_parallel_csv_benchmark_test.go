package hatSql

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkCH047CSVSerialBaseline(b *testing.B) {
	data := ch047ParallelCSVData(20_000)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows := 0
		err := StreamCSV(bytes.NewReader(data), ExternalImportOptions{}, func(_ []string, _ []string) error {
			rows++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if rows != 20_000 {
			b.Fatalf("rows = %d, want 20000", rows)
		}
	}
}

func BenchmarkCH047CSVSerialMaterializedBaseline(b *testing.B) {
	data := ch047ParallelCSVData(20_000)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var columns []string
		rows := make([]Row, 0, 20_000)
		err := StreamCSV(bytes.NewReader(data), ExternalImportOptions{}, func(recordColumns []string, record []string) error {
			if columns == nil {
				columns = append([]string(nil), recordColumns...)
			}
			row := make(Row, len(recordColumns))
			for columnIndex, column := range recordColumns {
				row[column] = record[columnIndex]
			}
			rows = append(rows, row)
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if len(columns) != 3 || len(rows) != 20_000 {
			b.Fatalf("columns/rows = %d/%d, want 3/20000", len(columns), len(rows))
		}
	}
}

func BenchmarkCH047CSVParallel(b *testing.B) {
	data := ch047ParallelCSVData(20_000)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		columns, rows, err := ParseCSVParallel(data, ExternalImportOptions{}, 4)
		if err != nil {
			b.Fatal(err)
		}
		if len(columns) != 3 || len(rows) != 20_000 {
			b.Fatalf("columns/rows = %d/%d, want 3/20000", len(columns), len(rows))
		}
	}
}

func ch047ParallelCSVData(rows int) []byte {
	data := []byte("id,state,note\n")
	for index := 0; index < rows; index++ {
		data = fmt.Appendf(data, "%d,state-%d,\"note %d, with comma\"\n", index, index%8, index)
	}
	return data
}
