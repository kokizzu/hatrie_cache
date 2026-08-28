package hatSql

import (
	"context"
	"testing"
)

func TestSQLNullSemanticsFunctionsAndCase(t *testing.T) {
	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM VALUES (NULL, 2) AS values(a, b) SELECT COALESCE(a, b) AS fallback, NULLIF(b, 2) AS absent, CASE WHEN a IS NULL THEN 'empty' ELSE 'present' END AS label`, nil, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %#v", result.Rows)
	}
	row := result.Rows[0]
	if row["fallback"] != int64(2) || row["absent"] != nil || row["label"] != "empty" {
		t.Fatalf("row = %#v", row)
	}
}

func TestSQLNullSemanticsThreeValuedPredicates(t *testing.T) {
	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM VALUES (NULL), (TRUE), (FALSE) AS values(flag) WHERE flag OR FALSE SELECT flag`, nil, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["flag"] != true {
		t.Fatalf("WHERE three-valued result = %#v", result.Rows)
	}
}

func TestSQLNullSemanticsTruthTable(t *testing.T) {
	for _, test := range []struct {
		operator string
		left     interface{}
		right    interface{}
		want     interface{}
	}{
		{"AND", nil, true, nil},
		{"AND", nil, false, false},
		{"OR", nil, true, true},
		{"OR", nil, false, nil},
		{"=", nil, int64(1), nil},
	} {
		if got := sqlBinaryValue(test.operator, test.left, test.right); got != test.want {
			t.Fatalf("%v %s %v = %#v, want %#v", test.left, test.operator, test.right, got, test.want)
		}
	}
}
