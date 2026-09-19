package hatSql_test

import (
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkTypedTableAggregateArrangementCheckpoint(b *testing.B) {
	arrangement, checkpoint, table, definition, changes := benchmarkM055AggregateCheckpointFixture(b)

	b.Run("capture", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := arrangement.CaptureCheckpoint(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("restore", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := arrangement.RestoreCheckpoint(checkpoint); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("rebuild", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			aggregate, err := hatSql.NewTypedTableAggregate(table, definition)
			if err != nil {
				b.Fatal(err)
			}
			if err := aggregate.Apply(changes); err != nil {
				b.Fatal(err)
			}
		}
	})

	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("json_decode", func(b *testing.B) {
		b.ReportMetric(float64(len(encoded)), "checkpoint-bytes")
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var decoded hatSql.TypedTableAggregateArrangementCheckpoint
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("json_roundtrip", func(b *testing.B) {
		b.ReportMetric(float64(len(encoded)), "checkpoint-bytes")
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			encoded, err := json.Marshal(checkpoint)
			if err != nil {
				b.Fatal(err)
			}
			var decoded hatSql.TypedTableAggregateArrangementCheckpoint
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkM055AggregateCheckpointFixture(b *testing.B) (*hatSql.TypedTableAggregateArrangement, hatSql.TypedTableAggregateArrangementCheckpoint, *hatSql.TypedTable, hatSql.TypedTableAggregateDefinition, []hatSql.TypedTableChange) {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "benchmark_scores",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
			{Name: "name", Kind: hatSql.TypedTableString},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 10_000; index++ {
		_, err := table.Upsert(
			"row-"+benchmarkM055Decimal(index),
			[]hatSql.TypedTableValue{
				hatSql.TypedString("team-" + benchmarkM055Decimal(index%32)),
				hatSql.TypedInt64(int64(index % 997)),
				hatSql.TypedString("name-" + benchmarkM055Decimal(index%4096)),
			},
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	definition := hatSql.TypedTableAggregateDefinition{
		GroupBy:       []string{"team"},
		SumField:      "points",
		MinField:      "points",
		MaxField:      "points",
		DistinctField: "name",
	}
	arrangement, err := arrangements.Acquire(definition)
	if err != nil {
		b.Fatal(err)
	}
	for {
		hydration, err := arrangement.Hydrate(1024)
		if err != nil {
			b.Fatal(err)
		}
		if hydration.Complete {
			break
		}
	}
	checkpoint, err := arrangement.CaptureCheckpoint()
	if err != nil {
		b.Fatal(err)
	}
	changes, _, err := table.ChangesAfter(0, 10_001)
	if err != nil {
		b.Fatal(err)
	}
	return arrangement, checkpoint, table, definition, changes
}

func benchmarkM055Decimal(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}
