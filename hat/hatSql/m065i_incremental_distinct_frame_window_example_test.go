package hatSql_test

import (
	"fmt"

	"hatrie_cache/hat/hatSql"
)

func ExampleIncrementalFrameWindow_countDistinct() {
	window, err := hatSql.NewIncrementalFrameWindow(hatSql.IncrementalFrameWindowDefinition{
		Kind:           hatSql.IncrementalWindowFrameCountDistinctInt64,
		OutputColumn:   "distinct_count",
		FramePreceding: 2,
		OrderKey:       func(row hatSql.Row) (interface{}, error) { return row["sequence"], nil },
		RowKey:         func(row hatSql.Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row hatSql.Row) (interface{}, error) { return row["tag"], nil },
	})
	if err != nil {
		panic(err)
	}
	updates, err := window.Append([]hatSql.Row{
		{"id": "a", "sequence": int64(1), "tag": int64(7)},
		{"id": "b", "sequence": int64(2), "tag": int64(7)},
		{"id": "c", "sequence": int64(3), "tag": nil},
		{"id": "d", "sequence": int64(4), "tag": int64(9)},
	})
	if err != nil {
		panic(err)
	}
	for _, update := range updates {
		fmt.Println(update.Row["distinct_count"])
	}
	// Output:
	// 1
	// 1
	// 1
	// 2
}
