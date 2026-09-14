package hatSql

import (
	"strconv"
	"testing"
	"time"
)

const ch007TTLBenchmarkRows = 4096

var ch007TTLBenchmarkSink int

func BenchmarkCH007TypedTableRows(b *testing.B) {
	benchmarkCases := []struct {
		name string
		ttl  TypedTableTTLOptions
	}{
		{name: "Disabled"},
		{
			name: "ProcessingTime",
			ttl: TypedTableTTLOptions{
				Mode:     TypedTableTTLProcessingTime,
				Lifetime: time.Hour,
				Clock:    func() time.Time { return time.Unix(100, 0) },
			},
		},
		{
			name: "EventTime",
			ttl: TypedTableTTLOptions{
				Mode:     TypedTableTTLEventTime,
				Field:    "event_time",
				Lifetime: time.Hour,
				Clock:    func() time.Time { return time.Unix(100, 0) },
			},
		},
	}
	for _, benchmarkCase := range benchmarkCases {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			table := newCH007TTLBenchmarkTable(b, benchmarkCase.ttl)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				ch007TTLBenchmarkSink += len(table.Rows())
			}
		})
	}
}

func BenchmarkCH007TypedTableCardinality(b *testing.B) {
	benchmarkCases := []struct {
		name string
		ttl  TypedTableTTLOptions
	}{
		{name: "Disabled"},
		{
			name: "ProcessingTime",
			ttl: TypedTableTTLOptions{
				Mode:     TypedTableTTLProcessingTime,
				Lifetime: time.Hour,
				Clock:    func() time.Time { return time.Unix(100, 0) },
			},
		},
		{
			name: "EventTime",
			ttl: TypedTableTTLOptions{
				Mode:     TypedTableTTLEventTime,
				Field:    "event_time",
				Lifetime: time.Hour,
				Clock:    func() time.Time { return time.Unix(100, 0) },
			},
		},
	}
	for _, benchmarkCase := range benchmarkCases {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			table := newCH007TTLBenchmarkTable(b, benchmarkCase.ttl)
			deadlineBytes := 0
			if table.ttl != nil {
				deadlineBytes = len(table.ttl.deadlines) * 8
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.ReportMetric(float64(deadlineBytes), "ttl_deadline_bytes")
			for index := 0; index < b.N; index++ {
				rows, _, _, err := table.SQLSourceCardinality("CACHE", "events")
				if err != nil {
					b.Fatal(err)
				}
				ch007TTLBenchmarkSink += rows
			}
		})
	}
}

func newCH007TTLBenchmarkTable(b *testing.B, ttl TypedTableTTLOptions) *TypedTable {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL:  ttl,
		Columns: []TypedTableColumn{
			{Name: "event_time", Kind: TypedTableInt64},
			{Name: "value", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	eventTime := TypedInt64(time.Unix(100, 0).UnixNano())
	for index := 0; index < ch007TTLBenchmarkRows; index++ {
		if _, err := table.Upsert(
			"row-"+strconv.Itoa(index),
			[]TypedTableValue{eventTime, TypedInt64(int64(index))},
		); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
