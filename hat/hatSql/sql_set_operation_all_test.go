package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestSQLIntersectAllPreservesDuplicateCountsAndLeftOrder(t *testing.T) {
	query := `FROM VALUES (2), (1), (1), (3), (1) AS lhs(id) SELECT lhs.id
INTERSECT ALL
FROM VALUES (1), (1), (4) AS rhs(id) SELECT rhs.id`
	result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []SQLRow{{"id": int64(1)}, {"id": int64(1)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLExceptAllPreservesRemainingDuplicateCountsAndLeftOrder(t *testing.T) {
	query := `FROM VALUES (2), (1), (1), (3), (1) AS lhs(id) SELECT lhs.id
EXCEPT ALL
FROM VALUES (1), (4) AS rhs(id) SELECT rhs.id`
	result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []SQLRow{{"id": int64(2)}, {"id": int64(1)}, {"id": int64(3)}, {"id": int64(1)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLSetOperationAllPreservesNullMultiplicity(t *testing.T) {
	query := `FROM VALUES (NULL), (NULL), (1) AS lhs(id) SELECT lhs.id
INTERSECT ALL
FROM VALUES (NULL), (1), (1) AS rhs(id) SELECT rhs.id`
	result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []SQLRow{{"id": nil}, {"id": int64(1)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLSetOperationAllHonorsCollationForMultiplicity(t *testing.T) {
	query := `FROM VALUES ('A'), ('A'), ('b') AS lhs(value) SELECT lhs.value
INTERSECT ALL
FROM VALUES ('a'), ('B'), ('B') AS rhs(value) SELECT rhs.value`
	result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{Collation: SQLCollationUnicodeCI})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []SQLRow{{"value": "A"}, {"value": "b"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}
