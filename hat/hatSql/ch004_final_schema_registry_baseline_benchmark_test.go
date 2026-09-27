package hatSql

import (
	"fmt"
	"testing"
)

var ch004FinalSchemaRegistryBenchmarkSink []SQLRow

func BenchmarkCH004FinalSchemaRegistryBaseline(b *testing.B) {
	rows := ch004FinalSchemaRegistryBenchmarkRows()
	options := SQLFinalOptions{
		Mode: SQLFinalReplacing,
		Key: func(row SQLRow) string {
			return row["id"].(string)
		},
		Version: func(row SQLRow) (uint64, error) {
			return row["version"].(uint64), nil
		},
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := applySQLFinalOptions(options, rows)
		if err != nil {
			b.Fatal(err)
		}
		ch004FinalSchemaRegistryBenchmarkSink = result
	}
}

func ch004FinalSchemaRegistryBenchmarkRows() []SQLRow {
	rows := make([]SQLRow, 256)
	for index := range rows {
		rows[index] = SQLRow{
			"id":      fmt.Sprintf("key-%03d", index/2),
			"version": uint64(index),
			"value":   int64(index),
		}
	}
	return rows
}
