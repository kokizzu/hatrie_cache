package hatSql

import (
	"context"
	"testing"
)

type sqlQueryRowsTestResolver struct{ rows []Row }

func (resolver sqlQueryRowsTestResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	return resolver.rows, nil
}

func TestSQLBatchLeafPredicatePreservesNullAndLiteralValues(t *testing.T) {
	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM VALUES (1), (NULL), (3) AS values(score) WHERE score >= 2 SELECT score, 5 AS constant`, nil, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %#v", result.Rows)
	}
	row := result.Rows[0]
	if row["score"] != int64(3) || row["constant"] != int64(5) {
		t.Fatalf("row = %#v", row)
	}
}

func TestSQLGroupRowsWithoutAggregatePreservesInputRows(t *testing.T) {
	rows := []sqlExecRow{
		newSQLSingleSourceExecRowAt("value", SQLRow{"id": int64(1)}, 0),
		newSQLSingleSourceExecRowAt("value", SQLRow{"id": int64(2)}, 1),
	}
	groups, err := groupSQLRows(rows, nil, &sqlQuery{})
	if err != nil {
		t.Fatalf("groupSQLRows() error = %v", err)
	}
	if len(groups) != len(rows) {
		t.Fatalf("groups = %d, want %d", len(groups), len(rows))
	}
	for index := range rows {
		if len(groups[index]) != 1 || groups[index][0].singleRow["id"] != rows[index].singleRow["id"] {
			t.Fatalf("group %d = %#v, want row %#v", index, groups[index], rows[index])
		}
	}
}

func TestSQLSimpleFieldLiteralPredicateMatchesBatchEvaluator(t *testing.T) {
	expression := sqlExpr{
		kind: "binary",
		op:   ">=",
		left: &sqlExpr{kind: "field", qualifier: "value", name: "score"},
		right: &sqlExpr{kind: "literal", value: int64(2)},
	}
	rows := []sqlExecRow{
		newSQLSingleSourceExecRowAt("value", SQLRow{"score": int64(1)}, 0),
		newSQLSingleSourceExecRowAt("value", SQLRow{"score": nil}, 1),
		newSQLSingleSourceExecRowAt("value", SQLRow{"score": int64(3)}, 2),
	}
	values, err := evalSQLExprBatch(expression, rows, nil)
	if err != nil {
		t.Fatalf("evalSQLExprBatch() error = %v", err)
	}
	if len(values) != 3 || values[0] != false || values[1] != nil || values[2] != true {
		t.Fatalf("predicate values = %#v", values)
	}
}

func TestSQLQueryRowsMatchesMaterializedSimpleFilter(t *testing.T) {
	resolver := sqlQueryRowsTestResolver{rows: []Row{{"id": int64(1), "score": int64(1)}, {"id": int64(2), "score": int64(2)}, {"id": int64(3), "score": int64(3)}}}
	const query = "FROM CACHE('events') AS event WHERE event.score >= 2 SELECT event.id"
	materialized, err := ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	var columns []string
	streamed := make([]SQLRow, 0)
	err = ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(gotColumns []string, row SQLRow) error {
		columns = append(columns[:0], gotColumns...)
		streamed = append(streamed, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if len(columns) != len(materialized.Columns) || columns[0] != materialized.Columns[0] {
		t.Fatalf("columns = %#v, want %#v", columns, materialized.Columns)
	}
	if len(streamed) != len(materialized.Rows) || streamed[0]["id"] != materialized.Rows[0]["id"] || streamed[1]["id"] != materialized.Rows[1]["id"] {
		t.Fatalf("streamed rows = %#v, want %#v", streamed, materialized.Rows)
	}
}

func TestSQLStreamSimpleFieldLiteralExpressionMatchesBatch(t *testing.T) {
	expression := sqlExpr{
		kind: "binary",
		op:   ">=",
		left: &sqlExpr{kind: "field", qualifier: "event", name: "score"},
		right: &sqlExpr{kind: "literal", value: int64(2)},
	}
	row := newSQLSingleSourceExecRow("event", SQLRow{"score": int64(3)})
	streamed, err := evalSQLStreamExpr(expression, row, nil)
	if err != nil {
		t.Fatalf("evalSQLStreamExpr() error = %v", err)
	}
	batched, err := evalSQLExprBatch(expression, []sqlExecRow{row}, nil)
	if err != nil {
		t.Fatalf("evalSQLExprBatch() error = %v", err)
	}
	if len(batched) != 1 || streamed != batched[0] {
		t.Fatalf("streamed = %#v, batched = %#v", streamed, batched)
	}
}
