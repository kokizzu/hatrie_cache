package hatSql_test

import (
	"fmt"

	"hatrie_cache/hat/hatSql"
)

func ExampleNewIncrementalFrameWindow() {
	window, err := hatSql.NewIncrementalFrameWindow(hatSql.IncrementalFrameWindowDefinition{
		Kind:           hatSql.IncrementalWindowFrameSumInt64,
		OutputColumn:   "rolling_sum",
		FramePreceding: 2,
		PartitionKey: func(row hatSql.Row) (string, error) {
			return row["account"].(string), nil
		},
		OrderKey: func(row hatSql.Row) (interface{}, error) {
			return row["sequence"], nil
		},
		RowKey: func(row hatSql.Row) (string, error) {
			return row["id"].(string), nil
		},
		ValueKey: func(row hatSql.Row) (interface{}, error) {
			return row["amount"], nil
		},
	})
	if err != nil {
		panic(err)
	}
	updates, err := window.Append([]hatSql.Row{
		{"id": "a", "account": "one", "sequence": int64(1), "amount": int64(5)},
		{"id": "b", "account": "one", "sequence": int64(2), "amount": nil},
		{"id": "c", "account": "one", "sequence": int64(3), "amount": int64(3)},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(updates[0].Row["rolling_sum"], updates[1].Row["rolling_sum"], updates[2].Row["rolling_sum"])
	// Output: 5 5 8
}
