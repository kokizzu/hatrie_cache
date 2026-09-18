package hatSql

import (
	"strconv"
	"testing"
	"time"
)

var ch009TTLRollupBenchmarkSink int

const ch009TTLRollupBenchmarkRows = 4096

func newCH009TTLRollupBenchmarkTable(now *time.Time, rows int) *TypedTable {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLProcessingTime,
			Lifetime: time.Second,
			Clock: func() time.Time {
				return *now
			},
		},
		Columns: []TypedTableColumn{
			{Name: "group", Kind: TypedTableString},
			{Name: "value", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		panic(err)
	}
	for index := 0; index < rows; index++ {
		if _, err := table.Upsert("row-"+strconv.Itoa(index), []TypedTableValue{
			TypedString("group-" + strconv.Itoa(index%16)),
			TypedInt64(int64(index)),
		}); err != nil {
			panic(err)
		}
	}
	return table
}

func BenchmarkCH009TTLPurgeOnly(b *testing.B) {
	for index := 0; index < b.N; index++ {
		now := time.Unix(5000, 0)
		table := newCH009TTLRollupBenchmarkTable(&now, ch009TTLRollupBenchmarkRows)
		now = now.Add(2 * time.Second)
		b.StartTimer()
		changes, err := table.PurgeExpired(now)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if len(changes) != ch009TTLRollupBenchmarkRows {
			b.Fatalf("purged changes = %d, want %d", len(changes), ch009TTLRollupBenchmarkRows)
		}
		ch009TTLRollupBenchmarkSink += len(changes)
	}
}

func BenchmarkCH009TTLPurgeWithRollup(b *testing.B) {
	for index := 0; index < b.N; index++ {
		now := time.Unix(5000, 0)
		table := newCH009TTLRollupBenchmarkTable(&now, ch009TTLRollupBenchmarkRows)
		rollup, err := NewTypedTableTTLRollup(table, TypedTableAggregateDefinition{
			GroupBy:  []string{"group"},
			SumField: "value",
		})
		if err != nil {
			b.Fatal(err)
		}
		now = now.Add(2 * time.Second)
		b.StartTimer()
		changes, err := table.PurgeExpired(now)
		if err == nil {
			err = rollup.ApplyExpired(changes)
		}
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if len(changes) != ch009TTLRollupBenchmarkRows {
			b.Fatalf("purged changes = %d, want %d", len(changes), ch009TTLRollupBenchmarkRows)
		}
		if got := len(rollup.Rows()); got != 16 {
			b.Fatalf("rollup groups = %d, want 16", got)
		}
		ch009TTLRollupBenchmarkSink += len(changes)
	}
}
