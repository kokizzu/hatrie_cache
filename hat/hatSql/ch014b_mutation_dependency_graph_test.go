package hatSql_test

import (
	"bytes"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCH014BClaimReadyNoReadyDoesNotAllocate(t *testing.T) {
	graph, err := newCH014BBlockedGraph()
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if claimed := graph.ClaimReady(1); len(claimed) != 0 {
			t.Fatalf("ClaimReady returned %d blocked tasks", len(claimed))
		}
	})
	if allocs != 0 {
		t.Fatalf("ClaimReady() allocations = %v, want 0", allocs)
	}
}

func TestCH014BCompletionMakesDependentsReadyInIDOrder(t *testing.T) {
	graph, err := hatSql.NewSQLMutationDependencyGraph(8)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range []hatSql.SQLMutationTask{
		{ID: "root"},
		{ID: "z", DependsOn: []string{"root"}},
		{ID: "a", DependsOn: []string{"root"}},
	} {
		if err := graph.Add(task); err != nil {
			t.Fatal(err)
		}
	}
	claimed := graph.ClaimReady(1)
	if len(claimed) != 1 || claimed[0].ID != "root" {
		t.Fatalf("root claim = %#v", claimed)
	}
	if err := graph.Complete("root", claimed[0].Attempt); err != nil {
		t.Fatal(err)
	}
	claimed = graph.ClaimReady(0)
	if len(claimed) != 2 || claimed[0].ID != "a" || claimed[1].ID != "z" {
		t.Fatalf("dependent claims = %#v, want a then z", claimed)
	}
}

func TestCH014BLoadRebuildsReadyQueue(t *testing.T) {
	source, err := hatSql.NewSQLMutationDependencyGraph(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Add(hatSql.SQLMutationTask{ID: "root"}); err != nil {
		t.Fatal(err)
	}
	if err := source.Add(hatSql.SQLMutationTask{ID: "child", DependsOn: []string{"root"}}); err != nil {
		t.Fatal(err)
	}
	root := source.ClaimReady(1)
	if len(root) != 1 {
		t.Fatalf("root claim = %#v", root)
	}
	if err := source.Complete("root", root[0].Attempt); err != nil {
		t.Fatal(err)
	}
	var snapshot bytes.Buffer
	if err := source.Save(&snapshot); err != nil {
		t.Fatal(err)
	}
	restored, err := hatSql.NewSQLMutationDependencyGraph(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Load(&snapshot); err != nil {
		t.Fatal(err)
	}
	claimed := restored.ClaimReady(1)
	if len(claimed) != 1 || claimed[0].ID != "child" {
		t.Fatalf("restored ready claim = %#v, want child", claimed)
	}
}
