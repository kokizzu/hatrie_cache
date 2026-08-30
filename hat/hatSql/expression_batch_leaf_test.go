package hatSql

import (
	"context"
	"testing"
)

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
