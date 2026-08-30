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
