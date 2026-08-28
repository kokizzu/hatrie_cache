package hatSql

import (
	"reflect"
	"testing"
)

func TestKeyGraphTraversalAndShortestPath(t *testing.T) {
	graph := NewKeyGraph()
	for _, edge := range []GraphEdge{
		{From: "account:1", To: "order:1"},
		{From: "account:1", To: "order:2"},
		{From: "order:1", To: "item:1"},
		{From: "order:2", To: "item:2"},
		{From: "item:2", To: "account:1"},
	} {
		if err := graph.Link(edge.From, edge.To); err != nil {
			t.Fatalf("Link(%+v): %v", edge, err)
		}
	}

	got, err := graph.Traverse("account:1", 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []GraphTraversal{
		{Node: "account:1", Depth: 0},
		{Node: "order:1", Depth: 1},
		{Node: "order:2", Depth: 1},
		{Node: "item:1", Depth: 2},
		{Node: "item:2", Depth: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Traverse() = %#v, want %#v", got, want)
	}

	path, ok, err := graph.ShortestPath("account:1", "item:2", 3)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !reflect.DeepEqual(path, []string{"account:1", "order:2", "item:2"}) {
		t.Fatalf("ShortestPath() = %v, %v", path, ok)
	}

	if _, ok, err := graph.ShortestPath("account:1", "item:2", 1); err != nil || ok {
		t.Fatalf("depth-limited ShortestPath() = ok:%v err:%v", ok, err)
	}
}

func TestKeyGraphMutationAndValidation(t *testing.T) {
	graph := NewKeyGraph()
	if err := graph.Link("a", "b"); err != nil {
		t.Fatal(err)
	}
	if err := graph.Link("a", "b"); err != nil {
		t.Fatal(err)
	}
	if !graph.Unlink("a", "b") || graph.Unlink("a", "b") {
		t.Fatal("Unlink presence reporting")
	}
	if got, err := graph.Traverse("a", 0); err != nil || !reflect.DeepEqual(got, []GraphTraversal{{Node: "a", Depth: 0}}) {
		t.Fatalf("Traverse zero depth = %#v, %v", got, err)
	}
	if err := graph.Link("", "b"); err == nil {
		t.Fatal("Link accepted empty key")
	}
	if _, err := graph.Traverse("a", -1); err == nil {
		t.Fatal("Traverse accepted negative depth")
	}
}
