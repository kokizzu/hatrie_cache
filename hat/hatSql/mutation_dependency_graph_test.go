package hatSql

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func TestSQLMutationDependencyGraphClaimsInDependencyOrderAndResumes(t *testing.T) {
	graph, err := NewSQLMutationDependencyGraph(8)
	if err != nil {
		t.Fatalf("NewSQLMutationDependencyGraph() error = %v", err)
	}
	for _, task := range []SQLMutationTask{
		{ID: "load"},
		{ID: "transform", DependsOn: []string{"load"}},
		{ID: "publish", DependsOn: []string{"transform"}},
	} {
		if err := graph.Add(task); err != nil {
			t.Fatalf("Add(%q) error = %v", task.ID, err)
		}
	}
	claimed := graph.ClaimReady(0)
	if len(claimed) != 1 || claimed[0].ID != "load" || claimed[0].State != SQLMutationTaskRunning || claimed[0].Attempt != 1 {
		t.Fatalf("first ClaimReady() = %#v, want load attempt 1", claimed)
	}
	if got := graph.ClaimReady(0); len(got) != 0 {
		t.Fatalf("ClaimReady() while dependency is running = %#v, want empty", got)
	}
	if err := graph.Complete("load", claimed[0].Attempt); err != nil {
		t.Fatalf("Complete(load) error = %v", err)
	}
	claimed = graph.ClaimReady(0)
	if len(claimed) != 1 || claimed[0].ID != "transform" {
		t.Fatalf("second ClaimReady() = %#v, want transform", claimed)
	}
	firstTransformAttempt := claimed[0].Attempt
	if err := graph.Fail("transform", firstTransformAttempt, "temporary failure"); err != nil {
		t.Fatalf("Fail(transform) error = %v", err)
	}
	if err := graph.Retry("transform"); err != nil {
		t.Fatalf("Retry(transform) error = %v", err)
	}
	claimed = graph.ClaimReady(0)
	if len(claimed) != 1 || claimed[0].ID != "transform" || claimed[0].Attempt != 2 {
		t.Fatalf("retry ClaimReady() = %#v, want transform attempt 2", claimed)
	}
	if err := graph.Complete("transform", firstTransformAttempt); !errors.Is(err, ErrSQLMutationDependencyGraphStaleAttempt) {
		t.Fatalf("stale Complete(transform) error = %v, want stale attempt", err)
	}
	if err := graph.Complete("transform", claimed[0].Attempt); err != nil {
		t.Fatalf("Complete(transform) error = %v", err)
	}
	claimed = graph.ClaimReady(0)
	if len(claimed) != 1 || claimed[0].ID != "publish" {
		t.Fatalf("final ClaimReady() = %#v, want publish", claimed)
	}
}

func TestSQLMutationDependencyGraphSnapshotRoundTripIsAtomic(t *testing.T) {
	graph, err := NewSQLMutationDependencyGraph(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Add(SQLMutationTask{ID: "base"}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Add(SQLMutationTask{ID: "derived", DependsOn: []string{"base"}}); err != nil {
		t.Fatal(err)
	}
	claimed := graph.ClaimReady(1)
	if len(claimed) != 1 {
		t.Fatalf("ClaimReady() = %#v, want one task", claimed)
	}
	if err := graph.Complete("base", claimed[0].Attempt); err != nil {
		t.Fatal(err)
	}
	want := graph.Snapshot()
	var encoded bytes.Buffer
	if err := graph.Save(&encoded); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	restored, err := NewSQLMutationDependencyGraph(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Load(bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored Snapshot() = %#v, want %#v", got, want)
	}
	beforeInvalid := restored.Snapshot()
	invalid := []byte(`{"version":1,"tasks":[{"id":"a","depends_on":["b"],"state":"pending"},{"id":"b","depends_on":["a"],"state":"pending"}]}`)
	if err := restored.Load(bytes.NewReader(invalid)); !errors.Is(err, ErrSQLMutationDependencyGraphCycle) {
		t.Fatalf("cyclic Load() error = %v, want cycle", err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, beforeInvalid) {
		t.Fatalf("cyclic Load() mutated graph: got %#v, want %#v", got, beforeInvalid)
	}
}

func TestSQLMutationDependencyGraphValidationAndRecovery(t *testing.T) {
	if got, err := NewSQLMutationDependencyGraph(0); err != nil || got == nil {
		t.Fatalf("default graph = %v, %v", got, err)
	}
	if _, err := NewSQLMutationDependencyGraph(-1); !errors.Is(err, ErrSQLMutationDependencyGraphInvalid) {
		t.Fatalf("negative max error = %v, want invalid", err)
	}
	graph, err := NewSQLMutationDependencyGraph(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Add(SQLMutationTask{ID: "one", DependsOn: []string{"missing"}}); !errors.Is(err, ErrSQLMutationDependencyGraphMissingDependency) {
		t.Fatalf("missing dependency error = %v, want missing dependency", err)
	}
	if err := graph.Add(SQLMutationTask{ID: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Add(SQLMutationTask{ID: "one"}); !errors.Is(err, ErrSQLMutationDependencyGraphDuplicate) {
		t.Fatalf("duplicate error = %v, want duplicate", err)
	}
	if err := graph.Add(SQLMutationTask{ID: "two"}); !errors.Is(err, ErrSQLMutationDependencyGraphCapacity) {
		t.Fatalf("capacity error = %v, want capacity", err)
	}
	claimed := graph.ClaimReady(1)
	if len(claimed) != 1 {
		t.Fatalf("ClaimReady() = %#v, want one task", claimed)
	}
	if graph.RequeueRunning() != 1 {
		t.Fatal("RequeueRunning() = 0, want one")
	}
	claimed = graph.ClaimReady(1)
	if len(claimed) != 1 || claimed[0].Attempt != 2 {
		t.Fatalf("requeued ClaimReady() = %#v, want attempt 2", claimed)
	}
}

func TestSQLMutationDependencyGraphConcurrentClaimsHaveSingleOwners(t *testing.T) {
	graph, err := NewSQLMutationDependencyGraph(32)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 32; index++ {
		if err := graph.Add(SQLMutationTask{ID: mutationDependencyTestID(index)}); err != nil {
			t.Fatal(err)
		}
	}
	claimed := make(chan SQLMutationTaskRecord, 32)
	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			batch := graph.ClaimReady(1)
			if len(batch) != 0 {
				claimed <- batch[0]
			}
		}()
	}
	wait.Wait()
	close(claimed)
	seen := make(map[string]struct{}, len(claimed))
	for task := range claimed {
		if _, exists := seen[task.ID]; exists {
			t.Fatalf("task %q was claimed more than once", task.ID)
		}
		seen[task.ID] = struct{}{}
	}
	if len(seen) != 32 {
		t.Fatalf("claimed %d tasks, want 32", len(seen))
	}
}

func mutationDependencyTestID(index int) string {
	return "task:" + strconv.Itoa(index)
}
