package hatCache

import (
	"context"
	"testing"
)

func TestSQLReadReplicaSetRoundRobinsSearchReads(t *testing.T) {
	first := newTestTrie(t)
	second := newTestTrie(t)
	if err := first.UpsertStringChecked("people", `[{"id":1}]`); err != nil {
		t.Fatal(err)
	}
	if err := second.UpsertStringChecked("people", `[{"id":2}]`); err != nil {
		t.Fatal(err)
	}
	replicas, err := NewSQLReadReplicaSet(first, second)
	if err != nil {
		t.Fatalf("NewSQLReadReplicaSet() error = %v", err)
	}
	query := "FROM CACHE('people') AS person SELECT person.id"
	firstResult, err := replicas.ExecuteSQLQuery(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil || firstResult.Rows[0]["id"] != float64(1) {
		t.Fatalf("first ExecuteSQLQuery() = %#v, %v", firstResult, err)
	}
	secondResult, err := replicas.ExecuteSQLQuery(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil || secondResult.Rows[0]["id"] != float64(2) {
		t.Fatalf("second ExecuteSQLQuery() = %#v, %v", secondResult, err)
	}
}
