package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkM065RankWindow(b *testing.B) {
	rows := benchmarkM065RankRows(1024)
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	})
	query := "FROM CACHE('items') AS item SELECT item.group, item.score, ROW_NUMBER() OVER (PARTITION BY item.group ORDER BY item.score) AS row_number"
	b.Run("full_recompute", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := ExecuteSQLQuery(query, resolver)
			if err != nil || len(result.Rows) != len(rows) {
				b.Fatalf("ExecuteSQLQuery() = %d rows, %v", len(result.Rows), err)
			}
		}
	})
	b.Run("incremental_append", func(b *testing.B) {
		window, err := NewIncrementalRankWindow(IncrementalRankWindowDefinition{
			Kind:         IncrementalWindowRowNumber,
			OutputColumn: "row_number",
			PartitionKey: benchmarkM065PartitionKey,
			OrderKey:     benchmarkM065OrderKey,
			RowKey:       benchmarkM065RowKey,
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := window.Append(rows); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			row := Row{
				"group": int64(index % 16),
				"score": int64(64 + index/16),
			}
			updates, err := window.Append([]Row{row})
			if err != nil || len(updates) != 1 {
				b.Fatalf("Append() = %#v, %v", updates, err)
			}
		}
	})
	b.Run("mutable_update", func(b *testing.B) {
		mutableRows := benchmarkM065MutableRankRows(1024)
		window, err := NewMutableIncrementalRankWindow(IncrementalRankWindowDefinition{
			Kind:         IncrementalWindowRowNumber,
			OutputColumn: "row_number",
			PartitionKey: benchmarkM065MutablePartitionKey,
			OrderKey:     benchmarkM065MutableOrderKey,
			RowKey:       benchmarkM065MutableRowKey,
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := window.Append(mutableRows); err != nil {
			b.Fatal(err)
		}
		mutation := []IncrementalRankWindowMutation{{
			Kind: IncrementalRankWindowUpdate,
			Key:  "519",
			Row:  Row{"id": "519", "group": int64(7), "score": int64(1)},
		}}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if index%2 == 0 {
				mutation[0].Row["score"] = int64(1)
			} else {
				mutation[0].Row["score"] = int64(62)
			}
			if _, err := window.Apply(mutation); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkM065PartitionKey(row Row) (string, error) {
	return strconv.FormatInt(row["group"].(int64), 10), nil
}

func benchmarkM065OrderKey(row Row) (interface{}, error) {
	return row["score"], nil
}

func benchmarkM065RowKey(row Row) (string, error) {
	return strconv.FormatInt(row["group"].(int64), 10) + ":" + strconv.FormatInt(row["score"].(int64), 10), nil
}

func benchmarkM065RankRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		rows[index] = Row{
			"group": int64(index % 16),
			"score": int64(index / 16),
		}
	}
	return rows
}

func benchmarkM065MutablePartitionKey(row Row) (string, error) {
	return strconv.FormatInt(row["group"].(int64), 10), nil
}

func benchmarkM065MutableOrderKey(row Row) (interface{}, error) {
	return row["score"], nil
}

func benchmarkM065MutableRowKey(row Row) (string, error) {
	return row["id"].(string), nil
}

func benchmarkM065MutableRankRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		rows[index] = Row{
			"id":    strconv.Itoa(index),
			"group": int64(index % 16),
			"score": int64(index / 16),
		}
	}
	return rows
}
