package hatPipeline

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestMutationDependencyGraphSchedulesDependenciesDeterministically(t *testing.T) {
	graph := NewMutationDependencyGraph()
	for _, id := range []string{"zeta", "alpha", "middle"} {
		if err := graph.Add(id); err != nil {
			t.Fatalf("Add(%q) error = %v", id, err)
		}
	}
	if err := graph.Add("dependent", "alpha", "middle"); err != nil {
		t.Fatalf("Add(dependent) error = %v", err)
	}
	if got, want := graph.Ready(), []string{"alpha", "middle", "zeta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Ready() = %#v, want %#v", got, want)
	}
	if err := graph.Start("middle"); err != nil {
		t.Fatalf("Start(middle) error = %v", err)
	}
	if got, want := graph.Ready(), []string{"alpha", "zeta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Ready() after Start = %#v, want %#v", got, want)
	}
	if err := graph.Complete("middle", nil); err != nil {
		t.Fatalf("Complete(middle) error = %v", err)
	}
	if got, want := graph.Ready(), []string{"alpha", "zeta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Ready() after middle completion = %#v, want %#v", got, want)
	}
	if err := graph.Start("alpha"); err != nil {
		t.Fatalf("Start(alpha) error = %v", err)
	}
	if err := graph.Complete("alpha", nil); err != nil {
		t.Fatalf("Complete(alpha) error = %v", err)
	}
	if got, want := graph.Ready(), []string{"dependent", "zeta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Ready() after all dependencies = %#v, want %#v", got, want)
	}
}

func TestMutationDependencyGraphBlocksFailureAndSupportsRetry(t *testing.T) {
	graph := NewMutationDependencyGraph()
	if err := graph.Add("source"); err != nil {
		t.Fatalf("Add(source) error = %v", err)
	}
	if err := graph.Add("index", "source"); err != nil {
		t.Fatalf("Add(index) error = %v", err)
	}
	if err := graph.Start("source"); err != nil {
		t.Fatalf("Start(source) error = %v", err)
	}
	failure := errors.New("source failed")
	if err := graph.Complete("source", failure); err != nil {
		t.Fatalf("Complete(source) error = %v", err)
	}
	status, ok := graph.Status("index")
	if !ok || status.State != MutationBlocked {
		t.Fatalf("Status(index) = %#v/%v, want blocked", status, ok)
	}
	if status.Error != "" {
		t.Fatalf("Status(index).Error = %q, want empty dependent error", status.Error)
	}
	if got := graph.Ready(); len(got) != 0 {
		t.Fatalf("Ready() after failure = %#v, want empty", got)
	}
	if err := graph.Retry("source"); err != nil {
		t.Fatalf("Retry(source) error = %v", err)
	}
	if got, want := graph.Ready(), []string{"source"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Ready() after retry = %#v, want %#v", got, want)
	}
	if err := graph.Start("source"); err != nil {
		t.Fatalf("Start(retried source) error = %v", err)
	}
	if err := graph.Complete("source", nil); err != nil {
		t.Fatalf("Complete(retried source) error = %v", err)
	}
	if got, want := graph.Ready(), []string{"index"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Ready() after successful retry = %#v, want %#v", got, want)
	}
}

func TestMutationDependencyGraphSnapshotResumesRunningWork(t *testing.T) {
	graph := NewMutationDependencyGraph()
	for _, id := range []string{"source", "index", "rollup"} {
		if id == "index" {
			if err := graph.Add(id, "source"); err != nil {
				t.Fatalf("Add(%q) error = %v", id, err)
			}
			continue
		}
		if id == "rollup" {
			if err := graph.Add(id, "index"); err != nil {
				t.Fatalf("Add(%q) error = %v", id, err)
			}
			continue
		}
		if err := graph.Add(id); err != nil {
			t.Fatalf("Add(%q) error = %v", id, err)
		}
	}
	if err := graph.Start("source"); err != nil {
		t.Fatalf("Start(source) error = %v", err)
	}
	if err := graph.Complete("source", nil); err != nil {
		t.Fatalf("Complete(source) error = %v", err)
	}
	if err := graph.Start("index"); err != nil {
		t.Fatalf("Start(index) error = %v", err)
	}
	snapshot := graph.Snapshot()
	restored, err := RestoreMutationDependencyGraph(snapshot)
	if err != nil {
		t.Fatalf("RestoreMutationDependencyGraph() error = %v", err)
	}
	if got, want := restored.Ready(), []string{"index"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("restored Ready() = %#v, want %#v", got, want)
	}
	status, ok := restored.Status("index")
	if !ok || status.State != MutationReady {
		t.Fatalf("restored Status(index) = %#v/%v, want ready", status, ok)
	}
	if err := restored.Start("index"); err != nil {
		t.Fatalf("Start(restored index) error = %v", err)
	}
	if err := restored.Complete("index", nil); err != nil {
		t.Fatalf("Complete(restored index) error = %v", err)
	}
	if got, want := restored.Ready(), []string{"rollup"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("restored Ready() after index = %#v, want %#v", got, want)
	}
}

func TestMutationDependencyGraphValidationAndProgress(t *testing.T) {
	graph := NewMutationDependencyGraph()
	if err := graph.Add("child", "missing"); !errors.Is(err, ErrMutationDependencyUnknown) {
		t.Fatalf("Add(child) error = %v, want unknown dependency", err)
	}
	if err := graph.Add("root"); err != nil {
		t.Fatalf("Add(root) error = %v", err)
	}
	if err := graph.Add("root"); !errors.Is(err, ErrMutationAlreadyExists) {
		t.Fatalf("duplicate Add(root) error = %v", err)
	}
	if err := graph.Add("child", "root", "root"); !errors.Is(err, ErrMutationDependencyDuplicate) {
		t.Fatalf("duplicate dependency error = %v", err)
	}
	if err := graph.Add("child", "root"); err != nil {
		t.Fatalf("Add(child) error = %v", err)
	}
	progress := graph.Progress()
	if progress.Total != 2 || progress.Ready != 1 || progress.Pending != 1 {
		t.Fatalf("Progress() = %#v, want total 2 ready 1 pending 1", progress)
	}
	if err := graph.Start("root"); err != nil {
		t.Fatalf("Start(root) error = %v", err)
	}
	if err := graph.Complete("root", nil); err != nil {
		t.Fatalf("Complete(root) error = %v", err)
	}
	if err := graph.Start("child"); err != nil {
		t.Fatalf("Start(child) error = %v", err)
	}
	if err := graph.Complete("child", nil); err != nil {
		t.Fatalf("Complete(child) error = %v", err)
	}
	progress = graph.Progress()
	if progress.Completed != 2 || progress.Total != 2 {
		t.Fatalf("completed Progress() = %#v, want completed 2 of 2", progress)
	}

	invalid := MutationDependencyGraphSnapshot{Nodes: []MutationNodeSnapshot{
		{ID: "a", Dependencies: []string{"b"}, State: MutationPending},
		{ID: "b", Dependencies: []string{"a"}, State: MutationPending},
	}}
	if _, err := RestoreMutationDependencyGraph(invalid); !errors.Is(err, ErrMutationDependencyCycle) {
		t.Fatalf("cycle restore error = %v, want dependency cycle", err)
	}
}

func ExampleMutationDependencyGraph() {
	graph := NewMutationDependencyGraph()
	graph.Add("base")
	graph.Add("index", "base")
	for _, id := range graph.Ready() {
		graph.Start(id)
		graph.Complete(id, nil)
	}
	fmt.Println(graph.Ready())
	// Output:
	// [index]
}

func BenchmarkMutationDependencyGraphReady(b *testing.B) {
	graph := NewMutationDependencyGraph()
	for index := 0; index < 4096; index++ {
		if err := graph.Add(fmt.Sprintf("mutation-%04d", index)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ready := graph.Ready()
		if len(ready) != 4096 {
			b.Fatalf("Ready() length = %d, want 4096", len(ready))
		}
	}
}
