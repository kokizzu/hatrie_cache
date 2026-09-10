package hatSql_test

import (
	"fmt"

	"hatrie_cache/hat/hatSql"
)

func ExampleNewMutableIncrementalFrameWindow() {
	window, err := hatSql.NewMutableIncrementalFrameWindow(hatSql.IncrementalFrameWindowDefinition{
		Kind:           hatSql.IncrementalWindowFrameSumInt64,
		OutputColumn:   "running_amount",
		FramePreceding: 1,
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
	changes, err := window.Apply([]hatSql.IncrementalFrameWindowMutation{
		{
			Kind: hatSql.IncrementalFrameWindowInsert,
			Key:  "row-1",
			Row:  hatSql.Row{"id": "row-1", "sequence": int64(1), "amount": int64(10)},
		},
		{
			Kind: hatSql.IncrementalFrameWindowInsert,
			Key:  "row-2",
			Row:  hatSql.Row{"id": "row-2", "sequence": int64(2), "amount": int64(20)},
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(changes[0].Row["running_amount"], changes[1].Row["running_amount"])
	// Output:
	// 10 30
}
