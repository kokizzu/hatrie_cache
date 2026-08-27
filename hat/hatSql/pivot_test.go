package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestPivotAndUnpivotRows(t *testing.T) {
	rows := []hatSql.Row{
		{"region": "east", "product": "book", "amount": 2.0},
		{"region": "east", "product": "pen", "amount": 3.0},
		{"region": "west", "product": "book", "amount": 5.0},
	}
	pivoted, err := hatSql.PivotRows(rows, hatSql.PivotSpec{
		GroupBy:     []string{"region"},
		PivotColumn: "product",
		ValueColumn: "amount",
		Values:      []string{"book", "pen"},
	})
	if err != nil {
		t.Fatalf("PivotRows error = %v", err)
	}
	if len(pivoted) != 2 || !sqlPivotRowExists(pivoted, "east", "book", 2.0, "pen", 3.0) || !sqlPivotRowExists(pivoted, "west", "book", 5.0, "pen", nil) {
		t.Fatalf("PivotRows rows = %#v, want one sparse row per region", pivoted)
	}

	unpivoted, err := hatSql.UnpivotRows(pivoted, hatSql.UnpivotSpec{
		Columns:     []string{"book", "pen"},
		NameColumn:  "product",
		ValueColumn: "amount",
	})
	if err != nil {
		t.Fatalf("UnpivotRows error = %v", err)
	}
	if len(unpivoted) != 3 || !sqlUnpivotRowExists(unpivoted, "east", "book", 2.0) || !sqlUnpivotRowExists(unpivoted, "east", "pen", 3.0) || !sqlUnpivotRowExists(unpivoted, "west", "book", 5.0) {
		t.Fatalf("UnpivotRows rows = %#v, want the original non-null facts", unpivoted)
	}
}

func TestPivotRowsAggregates(t *testing.T) {
	rows := []hatSql.Row{
		{"region": "east", "product": "book", "amount": 2.0},
		{"region": "east", "product": "book", "amount": 4.0},
		{"region": "east", "product": "pen", "amount": nil},
	}
	cases := []struct {
		name      string
		aggregate hatSql.PivotAggregate
		book      interface{}
		pen       interface{}
	}{
		{name: "sum", aggregate: hatSql.PivotSum, book: 6.0, pen: nil},
		{name: "average", aggregate: hatSql.PivotAverage, book: 3.0, pen: nil},
		{name: "minimum", aggregate: hatSql.PivotMinimum, book: 2.0, pen: nil},
		{name: "maximum", aggregate: hatSql.PivotMaximum, book: 4.0, pen: nil},
		{name: "count", aggregate: hatSql.PivotCount, book: 2, pen: 1},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			pivoted, err := hatSql.PivotRows(rows, hatSql.PivotSpec{
				GroupBy:     []string{"region"},
				PivotColumn: "product",
				ValueColumn: "amount",
				Aggregate:   testCase.aggregate,
			})
			if err != nil {
				t.Fatalf("PivotRows error = %v", err)
			}
			if len(pivoted) != 1 || pivoted[0]["book"] != testCase.book || pivoted[0]["pen"] != testCase.pen {
				t.Fatalf("PivotRows rows = %#v, want book=%#v pen=%#v", pivoted, testCase.book, testCase.pen)
			}
		})
	}
}

func sqlPivotRowExists(rows []hatSql.Row, region, firstName string, firstValue interface{}, secondName string, secondValue interface{}) bool {
	for _, row := range rows {
		if row["region"] == region && row[firstName] == firstValue && row[secondName] == secondValue {
			return true
		}
	}
	return false
}

func sqlUnpivotRowExists(rows []hatSql.Row, region, product string, amount float64) bool {
	for _, row := range rows {
		if row["region"] == region && row["product"] == product && row["amount"] == amount {
			return true
		}
	}
	return false
}
