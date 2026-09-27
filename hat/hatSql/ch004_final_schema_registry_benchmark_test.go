package hatSql

import (
	"fmt"
	"testing"
)

func BenchmarkCH004FinalSchemaRegistryResolveAndApply(b *testing.B) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Upsert("cache", "events", SQLFinalSchemaDefinition{
		Mode:         SQLFinalReplacing,
		KeyFields:    []string{"id"},
		VersionField: "version",
	}); err != nil {
		b.Fatal(err)
	}
	rows := ch004FinalSchemaRegistryBenchmarkRows()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		options, configured, err := registry.Resolve("cache", "events")
		if err != nil {
			b.Fatal(err)
		}
		if !configured {
			b.Fatal("registry definition was not configured")
		}
		result, err := applySQLFinalOptions(options, rows)
		if err != nil {
			b.Fatal(err)
		}
		ch004FinalSchemaRegistryBenchmarkSink = result
	}
}

func BenchmarkCH004FinalSchemaRegistryCompositeBaseline(b *testing.B) {
	rows := ch004FinalSchemaRegistryCompositeRows()
	options := SQLFinalOptions{
		Mode: SQLFinalReplacing,
		Key: func(row SQLRow) string {
			return row["tenant"].(string) + "\x00" + row["id"].(string)
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

func BenchmarkCH004FinalSchemaRegistryCompositeResolveAndApply(b *testing.B) {
	registry, err := NewSQLFinalSchemaRegistry(SQLFinalSchemaRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Upsert("cache", "events", SQLFinalSchemaDefinition{
		Mode:         SQLFinalReplacing,
		KeyFields:    []string{"tenant", "id"},
		VersionField: "version",
	}); err != nil {
		b.Fatal(err)
	}
	rows := ch004FinalSchemaRegistryCompositeRows()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		options, configured, err := registry.Resolve("cache", "events")
		if err != nil {
			b.Fatal(err)
		}
		if !configured {
			b.Fatal("registry definition was not configured")
		}
		result, err := applySQLFinalOptions(options, rows)
		if err != nil {
			b.Fatal(err)
		}
		ch004FinalSchemaRegistryBenchmarkSink = result
	}
}

func ch004FinalSchemaRegistryCompositeRows() []SQLRow {
	rows := make([]SQLRow, 256)
	for index := range rows {
		rows[index] = SQLRow{
			"tenant":  fmt.Sprintf("tenant-%02d", index/2),
			"id":      fmt.Sprintf("id-%03d", index/2),
			"version": uint64(index),
			"value":   int64(index),
		}
	}
	return rows
}
