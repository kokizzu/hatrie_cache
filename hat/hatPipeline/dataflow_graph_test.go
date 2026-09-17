package hatPipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestDataflowGraphSnapshotQueriesAndOrder(t *testing.T) {
	graph, err := NewDataflowGraph(DataflowGraphOptions{MaxNodes: 8, MaxEdges: 8})
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range []DataflowNode{
		{ID: "source", Kind: "source"},
		{ID: "filter", Kind: "filter"},
		{ID: "sink", Kind: "sink"},
	} {
		if err := graph.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}
	for _, edge := range []DataflowEdge{
		{From: "source", To: "filter"},
		{From: "filter", To: "sink"},
	} {
		if err := graph.AddEdge(edge); err != nil {
			t.Fatal(err)
		}
	}

	snapshot := graph.Snapshot()
	wantSnapshot := DataflowGraphSnapshot{
		Nodes: []DataflowNode{
			{ID: "filter", Kind: "filter"},
			{ID: "sink", Kind: "sink"},
			{ID: "source", Kind: "source"},
		},
		Edges: []DataflowEdge{
			{From: "filter", To: "sink"},
			{From: "source", To: "filter"},
		},
	}
	if !reflect.DeepEqual(snapshot, wantSnapshot) {
		t.Fatalf("snapshot = %#v, want %#v", snapshot, wantSnapshot)
	}

	dependencies, err := graph.Dependencies("sink")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"filter"}; !reflect.DeepEqual(dependencies, want) {
		t.Fatalf("dependencies = %v, want %v", dependencies, want)
	}
	dependents, err := graph.Dependents("filter")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"sink"}; !reflect.DeepEqual(dependents, want) {
		t.Fatalf("dependents = %v, want %v", dependents, want)
	}
	order, err := graph.TopologicalOrder()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"source", "filter", "sink"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	impact, err := graph.Impact("source")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"filter", "sink"}; !reflect.DeepEqual(impact, want) {
		t.Fatalf("impact = %v, want %v", impact, want)
	}

	snapshot.Nodes[0].ID = "changed"
	if node, ok := graph.Node("filter"); !ok || node.ID != "filter" {
		t.Fatalf("snapshot mutation changed graph: %#v, %v", node, ok)
	}
}

func TestDataflowGraphRejectsInvalidEdgesAndCapacity(t *testing.T) {
	graph, err := NewDataflowGraph(DataflowGraphOptions{MaxNodes: 2, MaxEdges: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.AddNode(DataflowNode{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddNode(DataflowNode{ID: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddNode(DataflowNode{ID: "c"}); !errors.Is(err, ErrDataflowGraphCapacity) {
		t.Fatalf("third node error = %v, want capacity", err)
	}
	if err := graph.AddEdge(DataflowEdge{From: "a", To: "missing"}); !errors.Is(err, ErrDataflowGraphNodeNotFound) {
		t.Fatalf("missing node error = %v, want node-not-found", err)
	}
	if err := graph.AddEdge(DataflowEdge{From: "a", To: "a"}); !errors.Is(err, ErrDataflowGraphCycle) {
		t.Fatalf("self edge error = %v, want cycle", err)
	}
	if err := graph.AddEdge(DataflowEdge{From: "a", To: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddEdge(DataflowEdge{From: "a", To: "b"}); !errors.Is(err, ErrDataflowGraphDuplicateEdge) {
		t.Fatalf("duplicate edge error = %v, want duplicate", err)
	}
	if err := graph.AddEdge(DataflowEdge{From: "b", To: "a"}); !errors.Is(err, ErrDataflowGraphCycle) {
		t.Fatalf("cycle edge error = %v, want cycle", err)
	}
}

func TestDataflowGraphRemovalAndNilSafety(t *testing.T) {
	graph, err := NewDataflowGraph(DataflowGraphOptions{MaxNodes: 8, MaxEdges: 8})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if err := graph.AddNode(DataflowNode{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if err := graph.AddEdge(DataflowEdge{From: "a", To: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddEdge(DataflowEdge{From: "b", To: "c"}); err != nil {
		t.Fatal(err)
	}
	if err := graph.RemoveEdge(DataflowEdge{From: "a", To: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := graph.RemoveNode("b"); err != nil {
		t.Fatal(err)
	}
	if graph.Len() != 2 || graph.EdgeCount() != 0 {
		t.Fatalf("lengths = %d nodes, %d edges", graph.Len(), graph.EdgeCount())
	}
	if _, err := graph.Dependencies("missing"); !errors.Is(err, ErrDataflowGraphNodeNotFound) {
		t.Fatalf("unknown dependencies error = %v", err)
	}

	var nilGraph *DataflowGraph
	if err := nilGraph.AddNode(DataflowNode{ID: "x"}); !errors.Is(err, ErrDataflowGraphNil) {
		t.Fatalf("nil add error = %v, want nil", err)
	}
	if got := nilGraph.Snapshot(); len(got.Nodes) != 0 || len(got.Edges) != 0 {
		t.Fatalf("nil snapshot = %#v", got)
	}
}

func TestPipelineDescribeIsDetachedAndStable(t *testing.T) {
	pipeline, err := NewPipeline(
		Stage[int]{Name: "read", Workers: 2, Queue: 4, Process: func(_ context.Context, value int) (int, error) { return value, nil }},
		Stage[int]{Name: "write", Workers: 3, Queue: 5, Process: func(_ context.Context, value int) (int, error) { return value, nil }},
	)
	if err != nil {
		t.Fatal(err)
	}
	description := pipeline.Describe()
	want := PipelineDescription{
		Stages: []PipelineStageDescription{
			{ID: "stage-1", Name: "read", Workers: 2, Queue: 4},
			{ID: "stage-2", Name: "write", Workers: 3, Queue: 5},
		},
		Edges: []DataflowEdge{{From: "stage-1", To: "stage-2"}},
	}
	if !reflect.DeepEqual(description, want) {
		t.Fatalf("description = %#v, want %#v", description, want)
	}
	description.Stages[0].Name = "changed"
	if got := pipeline.Describe().Stages[0].Name; got != "read" {
		t.Fatalf("description mutation changed pipeline: %q", got)
	}
}

func TestDataflowGraphConcurrentQueriesAndMutation(t *testing.T) {
	graph, err := NewDataflowGraph(DataflowGraphOptions{MaxNodes: 64, MaxEdges: 128})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 32; index++ {
		if err := graph.AddNode(DataflowNode{ID: dataflowTestNodeID(index)}); err != nil {
			t.Fatal(err)
		}
	}

	start := make(chan struct{})
	errorsCh := make(chan error, 4096)
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			for index := 0; index < 200; index++ {
				if _, err := graph.TopologicalOrder(); err != nil {
					errorsCh <- err
				}
				if _, err := graph.Dependencies(dataflowTestNodeID(index % 32)); err != nil {
					errorsCh <- err
				}
				if _, err := graph.Dependents(dataflowTestNodeID(index % 32)); err != nil {
					errorsCh <- err
				}
				_ = graph.Snapshot()
			}
		}()
	}
	for worker := 0; worker < 4; worker++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			<-start
			for index := worker; index < 31; index += 4 {
				if err := graph.AddEdge(DataflowEdge{From: dataflowTestNodeID(index), To: dataflowTestNodeID(index + 1)}); err != nil {
					errorsCh <- err
				}
			}
		}(worker)
	}
	close(start)
	waitGroup.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Errorf("concurrent graph operation failed: %v", err)
	}
	if graph.EdgeCount() != 31 {
		t.Fatalf("edge count = %d, want 31", graph.EdgeCount())
	}
}

func TestNewDataflowGraphRejectsInvalidLimitsAndIdentifiers(t *testing.T) {
	for _, options := range []DataflowGraphOptions{
		{MaxNodes: -1},
		{MaxEdges: -1},
		{MaxNodes: maxDataflowGraphNodes + 1},
		{MaxEdges: maxDataflowGraphEdges + 1},
	} {
		if _, err := NewDataflowGraph(options); !errors.Is(err, ErrDataflowGraphInvalid) {
			t.Fatalf("options %#v error = %v, want invalid", options, err)
		}
	}
	graph, err := NewDataflowGraph(DataflowGraphOptions{MaxNodes: 2, MaxEdges: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.AddNode(DataflowNode{}); !errors.Is(err, ErrDataflowGraphInvalid) {
		t.Fatalf("empty node error = %v, want invalid", err)
	}
	if err := graph.AddNode(DataflowNode{ID: " source ", Kind: " "}); err != nil {
		t.Fatal(err)
	}
	node, ok := graph.Node("source")
	if !ok || node.Kind != "operator" {
		t.Fatalf("normalized node = %#v, %v", node, ok)
	}
}

func ExampleDataflowGraph() {
	graph, err := NewDataflowGraph(DataflowGraphOptions{MaxNodes: 4, MaxEdges: 4})
	if err != nil {
		panic(err)
	}
	for _, node := range []DataflowNode{
		{ID: "orders", Kind: "source"},
		{ID: "daily-total", Kind: "aggregate"},
		{ID: "warehouse", Kind: "sink"},
	} {
		if err := graph.AddNode(node); err != nil {
			panic(err)
		}
	}
	if err := graph.AddEdge(DataflowEdge{From: "orders", To: "daily-total"}); err != nil {
		panic(err)
	}
	if err := graph.AddEdge(DataflowEdge{From: "daily-total", To: "warehouse"}); err != nil {
		panic(err)
	}
	affected, err := graph.Impact("orders")
	if err != nil {
		panic(err)
	}
	fmt.Println(strings.Join(affected, ","))
	// Output: daily-total,warehouse
}

func dataflowTestNodeID(index int) string {
	return "node-" + string(rune('a'+index/26)) + string(rune('a'+index%26))
}
