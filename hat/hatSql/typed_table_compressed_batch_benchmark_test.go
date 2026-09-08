package hatSql_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var compressedBatchBenchmarkSink hatSql.ColumnarBatch

func BenchmarkTypedTableColumnarCompressedBatches(b *testing.B) {
	for _, compressed := range []struct {
		name  string
		value bool
	}{
		{name: "legacy", value: false},
		{name: "compressed", value: true},
	} {
		b.Run(compressed.name, func(b *testing.B) {
			sample := newCompressedBatchBenchmarkBatch()
			sample.EncodeRepeatedStrings()
			if compressed.value {
				sample.PackCompressedColumns()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				batch := newCompressedBatchBenchmarkBatch()
				batch.EncodeRepeatedStrings()
				if compressed.value {
					batch.PackCompressedColumns()
				}
				compressedBatchBenchmarkSink = batch
			}
			b.ReportMetric(float64(compressedBatchBenchmarkLayoutBytes(sample)), "payload-bytes/op")
		})
	}
}

func BenchmarkTypedTableColumnarCompressedBatchesQuery(b *testing.B) {
	for _, compressed := range []struct {
		name  string
		value bool
	}{
		{name: "legacy", value: false},
		{name: "compressed", value: true},
	} {
		b.Run(compressed.name, func(b *testing.B) {
			table := newCompressedBatchBenchmarkTable(b, compressed.value)
			fields := []string{"team", "points", "active", "notes"}
			if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !available {
				b.Fatalf("warm ResolveSQLColumnarSource() available = %t, error = %v", available, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT team, points WHERE points >= 6000", table, nil, hatSql.QueryOptions{})
				if err != nil || len(result.Rows) == 0 {
					b.Fatalf("query result = %#v, error = %v", result, err)
				}
			}
		})
	}
}

func newCompressedBatchBenchmarkTable(b *testing.B, compressed bool) *hatSql.TypedTable {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
			{Name: "active", Kind: hatSql.TypedTableBool},
			{Name: "notes", Kind: hatSql.TypedTableString},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled: true,
			CompressedBatches: compressed,
			MaxBytes: 16 << 20,
			MinReads: 1,
			RowsPerSegment: 256,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := range 4096 {
		team := string(rune('a' + index%16))
		note := string(rune('a' + index%4))
		if _, err := table.Upsert(strconv.Itoa(index), []hatSql.TypedTableValue{
			hatSql.TypedString(team),
			hatSql.TypedInt64(int64(index * 3)),
			hatSql.TypedBool(index%3 != 0),
			hatSql.TypedString(note),
		}); err != nil {
			b.Fatal(err)
		}
	}
	return table
}

func newCompressedBatchBenchmarkBatch() hatSql.ColumnarBatch {
	const rows = 4096
	teams := make([]interface{}, rows)
	points := make([]interface{}, rows)
	active := make([]interface{}, rows)
	notes := make([]interface{}, rows)
	for index := range rows {
		teams[index] = strings.Repeat("team-", 8) + string(rune('a'+index%16))
		points[index] = int64(index * 3)
		active[index] = index%3 != 0
		if index%10 == 0 {
			notes[index] = strings.Repeat("note-", 12) + string(rune('a'+index%4))
		}
	}
	return hatSql.ColumnarBatch{
		Columns: map[string][]interface{}{
			"team":   teams,
			"points": points,
			"active": active,
			"notes":  notes,
		},
		Rows: rows,
	}
}

func compressedBatchBenchmarkLayoutBytes(batch hatSql.ColumnarBatch) int {
	bytes := 0
	for _, values := range batch.Columns {
		bytes += len(values) * 16
	}
	for _, values := range batch.Dictionaries {
		if values.Values != nil {
			bytes += len(values.Values) * 16
			for _, value := range values.Values {
				bytes += len(value)
			}
		} else {
			bytes += len(values.PackedValueData) + len(values.ValueOffsets)*4
		}
		if values.Codes != nil {
			bytes += len(values.Codes) * 4
		} else {
			bytes += len(values.PackedCodes)
		}
	}
	for _, values := range batch.PackedColumns {
		bytes += len(values.Values)*16 + len(values.Validity) + len(values.Ranks)*4
	}
	for _, values := range batch.BoolColumns {
		bytes += len(values.Bits) + len(values.Validity)
	}
	for _, values := range batch.NumericColumns {
		bytes += len(values.Data) + len(values.Validity)
	}
	return bytes
}
