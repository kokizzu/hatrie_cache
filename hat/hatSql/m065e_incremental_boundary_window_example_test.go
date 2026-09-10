package hatSql_test

import (
	"fmt"

	"hatrie_cache/hat/hatSql"
)

func ExampleNewIncrementalBoundaryWindow() {
	window, err := hatSql.NewIncrementalBoundaryWindow(hatSql.IncrementalBoundaryWindowDefinition{
		Kind:         hatSql.IncrementalWindowFirstValue,
		OutputColumn: "first_value",
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
			return row["name"], nil
		},
	})
	if err != nil {
		panic(err)
	}
	updates, err := window.Append([]hatSql.Row{
		{"id": "a", "account": "one", "sequence": int64(1), "name": "Ada"},
		{"id": "b", "account": "one", "sequence": int64(2), "name": "Lin"},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(updates[0].Row["first_value"], updates[1].Row["first_value"])
	// Output: Ada Ada
}
