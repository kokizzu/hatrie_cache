package hatSql

import (
	"context"
	"strconv"
	"testing"
)

var ch007DecompressedBlockQueryResult SQLQueryResult

func BenchmarkCH007DecompressedColumnBlockCache(b *testing.B) {
	query := "FROM CACHE('events') SELECT id, id WHERE id >= 0"
	ctx := context.Background()
	for _, benchmark := range []struct {
		name    string
		enabled bool
	}{
		{name: "disabled"},
		{name: "enabled", enabled: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			table := newCH007DecompressedBlockTable(b, benchmark.enabled)
			if _, err := ExecuteQueryParameters(ctx, query, table, nil, QueryOptions{}); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			b.ReportMetric(float64(ch007DecompressedBlockCacheBytes(table)), "retained-cache-bytes")
			for index := 0; index < b.N; index++ {
				result, err := ExecuteQueryParameters(ctx, query, table, nil, QueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				ch007DecompressedBlockQueryResult = result
			}
		})
	}
}

func ch007DecompressedBlockCacheBytes(table *TypedTable) int {
	total := 0
	table.columnar.mu.Lock()
	defer table.columnar.mu.Unlock()
	for _, layout := range table.columnar.layouts {
		cache := layout.batch.decompressedBlockCache
		if cache == nil {
			continue
		}
		cache.mu.Lock()
		total += cache.bytes
		cache.mu.Unlock()
	}
	return total
}

func newCH007DecompressedBlockTable(b *testing.B, decompressedBlockCache bool) *TypedTable {
	b.Helper()
	const rowCount = 65536
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "id", Kind: TypedTableInt64}},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:                   true,
			CompressedBatches:         true,
			MaxBytes:                  4 << 20,
			MinReads:                  1,
			DecompressedBlockCache:    decompressedBlockCache,
			DecompressedBlockMaxBytes: 4 << 20,
			DecompressedBlockRows:     256,
			DecompressedBlockMinReads: 1,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < rowCount; index++ {
		if _, err := table.Upsert("key-"+strconv.Itoa(index), []TypedTableValue{TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
