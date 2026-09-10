package hatSql_test

import (
	"fmt"

	"hatrie_cache/hat/hatSql"
)

func ExampleIncrementalFrameWindow_minMax() {
	window, err := hatSql.NewIncrementalFrameWindow(hatSql.IncrementalFrameWindowDefinition{
		Kind:           hatSql.IncrementalWindowFrameMinInt64,
		OutputColumn:   "rolling_min",
		FramePreceding: 1,
		OrderKey:       func(row hatSql.Row) (interface{}, error) { return row["sequence"], nil },
		RowKey:         func(row hatSql.Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row hatSql.Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		panic(err)
	}
	updates, err := window.Append([]hatSql.Row{
		{"id": "a", "sequence": int64(1), "value": int64(5)},
		{"id": "b", "sequence": int64(2), "value": nil},
		{"id": "c", "sequence": int64(3), "value": int64(3)},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(updates[0].Row["rolling_min"], updates[1].Row["rolling_min"], updates[2].Row["rolling_min"])
	// Output: 5 5 3
}
