package hatSql

import "fmt"

func ExampleFilterDifferentialRows() {
	rows := []DifferentialRow{
		{Key: "user-1", Time: 10, Diff: 2, Row: Row{"active": true}},
		{Key: "user-1", Time: 10, Diff: -1, Row: Row{"active": true}},
	}

	active, err := FilterDifferentialRows(rows, func(row Row) (bool, error) {
		return row["active"] == true, nil
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	active, err = ConsolidateDifferentialRows(active)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(active[0].Diff)
	// Output: 1
}
