package hatSql

import (
	"errors"
	"testing"
)

func TestCH014BRollbackClaimRequeuesReadyTask(t *testing.T) {
	graph, err := NewSQLMutationDependencyGraph(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Add(SQLMutationTask{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	claimed := graph.ClaimReady(1)
	if len(claimed) != 1 {
		t.Fatalf("initial claims = %#v", claimed)
	}
	cause := errors.New("append failed")
	if err := rollbackSQLMutationDependencyGraphClaims(graph, claimed, cause); !errors.Is(err, cause) {
		t.Fatalf("rollback error = %v, want %v", err, cause)
	}
	reclaimed := graph.ClaimReady(1)
	if len(reclaimed) != 1 || reclaimed[0].ID != "root" || reclaimed[0].Attempt != 1 {
		t.Fatalf("reclaimed = %#v, want root attempt 1", reclaimed)
	}
}
