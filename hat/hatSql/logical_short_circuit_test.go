package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestSQLLogicalBatchShortCircuitPreservesThreeValuedLogic(t *testing.T) {
	t.Parallel()
	payload := sqlExpr{kind: "field", qualifier: "event", name: "payload"}
	pattern := sqlExpr{kind: "literal", value: "%hit%"}
	right := sqlExpr{kind: "binary", op: "LIKE", left: &payload, right: &pattern}
	active := sqlExpr{kind: "field", qualifier: "event", name: "active"}
	rows := []sqlExecRow{
		newSQLSingleSourceExecRow("event", SQLRow{"active": true, "payload": 123}),
		newSQLSingleSourceExecRow("event", SQLRow{"active": nil, "payload": "hit-value"}),
		newSQLSingleSourceExecRow("event", SQLRow{"active": nil, "payload": "miss-value"}),
		newSQLSingleSourceExecRow("event", SQLRow{"active": false, "payload": "hit-value"}),
		newSQLSingleSourceExecRow("event", SQLRow{"active": false, "payload": "miss-value"}),
	}
	for _, test := range []struct {
		name string
		op   string
		want []interface{}
	}{
		{name: "or", op: "OR", want: []interface{}{true, true, nil, true, false}},
		{name: "and", op: "AND", want: []interface{}{false, nil, false, false, false}},
	} {
		t.Run(test.name, func(t *testing.T) {
			expression := sqlExpr{kind: "binary", op: test.op, left: &active, right: &right}
			got, err := evalSQLExprBatch(expression, rows, nil)
			if err != nil {
				t.Fatalf("evalSQLExprBatch() error = %v", err)
			}
			for index, want := range test.want {
				if got[index] != want {
					t.Fatalf("row %d result = %#v, want %#v", index, got[index], want)
				}
			}
		})
	}
}

func TestSQLLogicalBatchShortCircuitSafetyBoundary(t *testing.T) {
	t.Parallel()
	safe := sqlExpr{kind: "binary", op: "LIKE", left: &sqlExpr{kind: "field"}, right: &sqlExpr{kind: "literal", value: "%x%"}}
	unsafe := sqlExpr{kind: "func", name: "custom", args: []sqlExpr{{kind: "field"}}}
	if !sqlExprBatchShortCircuitSafe(safe) {
		t.Fatal("LIKE was not admitted as a total short-circuit expression")
	}
	if sqlExprBatchShortCircuitSafe(unsafe) {
		t.Fatal("custom function was incorrectly admitted as a short-circuit expression")
	}
	left := sqlExpr{kind: "literal", value: true}
	right := sqlExpr{kind: "binary", op: "REGEXP", left: &sqlExpr{kind: "literal", value: "value"}, right: &sqlExpr{kind: "literal", value: "["}}
	expression := sqlExpr{kind: "binary", op: "OR", left: &left, right: &right}
	values, err := evalSQLExprBatch(expression, []sqlExecRow{{}}, nil)
	if err != nil {
		t.Fatalf("unsafe expression batch error = %v", err)
	}
	if len(values) != 1 || sqlExpressionError(values[0]) == nil {
		t.Fatalf("unsafe right expression was skipped: %#v", values)
	}
}

func TestSQLLogicalBatchShortCircuitColumnarQueryPreservesRows(t *testing.T) {
	t.Parallel()
	probe := &sqlSegmentedColumnarSourceProbe{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{
				"active":  {true, nil, nil, false, false},
				"payload": {"no-match", "hit-value", "miss-value", "hit-value", "miss-value"},
			},
			Rows: 5,
		},
	}
	query := "FROM CACHE('events') AS event WHERE event.active OR event.payload LIKE '%hit%' SELECT event.active, event.payload"
	result, err := ExecuteSQLQueryParameters(context.Background(), query, probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{
		{"active": true, "payload": "no-match"},
		{"active": nil, "payload": "hit-value"},
		{"active": false, "payload": "hit-value"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("columnar rows = %#v, want %#v", result.Rows, want)
	}
}
