package hatSql_test

import (
	"fmt"

	"hatrie_cache/hat/hatSql"
)

func ExampleNewIncrementalNthValueWindow() {
	window, err := hatSql.NewIncrementalNthValueWindow(hatSql.IncrementalNthValueWindowDefinition{
		Position:     2,
		OutputColumn: "second_name",
		OrderKey:     func(row hatSql.Row) (interface{}, error) { return row["sequence"], nil },
		RowKey:       func(row hatSql.Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row hatSql.Row) (interface{}, error) { return row["name"], nil },
	})
	if err != nil {
		panic(err)
	}
	updates, err := window.Append([]hatSql.Row{
		{"id": "a", "sequence": int64(1), "name": "Ada"},
		{"id": "b", "sequence": int64(2), "name": "Lin"},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(updates[0].Row["second_name"], updates[1].Row["second_name"])
	// Output: <nil> Lin
}
