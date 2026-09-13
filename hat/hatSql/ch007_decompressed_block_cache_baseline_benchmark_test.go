package hatSql

import (
	"context"
	"strconv"
	"testing"
)

var ch007BaselineQueryResult SQLQueryResult

func BenchmarkCH007BaselineDecompressedColumnBlockQuery(b *testing.B) {
	table := newCH007BaselineDecompressedBlockTable(b)
	query := "FROM CACHE('events') SELECT id, id WHERE id >= 0"
	ctx := context.Background()
	if _, err := ExecuteQueryParameters(ctx, query, table, nil, QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteQueryParameters(ctx, query, table, nil, QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch007BaselineQueryResult = result
	}
}

func newCH007BaselineDecompressedBlockTable(b *testing.B) *TypedTable {
	b.Helper()
	const rowCount = 65536
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "id", Kind: TypedTableInt64}},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:           true,
			CompressedBatches: true,
			MaxBytes:          4 << 20,
			MinReads:          1,
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
