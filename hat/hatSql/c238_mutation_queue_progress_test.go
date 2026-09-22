package hatSql

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestC238MutationDependencyGraphProgressCountsLifecycle(t *testing.T) {
	graph, err := NewSQLMutationDependencyGraph(4)
	if err != nil {
		t.Fatalf("NewSQLMutationDependencyGraph() error = %v", err)
	}
	if err := graph.Add(SQLMutationTask{ID: "root"}); err != nil {
		t.Fatalf("Add(root) error = %v", err)
	}
	if err := graph.Add(SQLMutationTask{ID: "child", DependsOn: []string{"root"}}); err != nil {
		t.Fatalf("Add(child) error = %v", err)
	}
	progress := graph.Progress()
	if progress.Total != 2 || progress.Ready != 1 || progress.Pending != 2 || progress.Blocked != 1 || progress.Remaining != 2 {
		t.Fatalf("initial progress = %#v, want total=2 ready=1 pending=2 blocked=1 remaining=2", progress)
	}
	claimed := graph.ClaimReady(0)
	if len(claimed) != 1 || claimed[0].ID != "root" {
		t.Fatalf("ClaimReady() = %#v, want root", claimed)
	}
	progress = graph.Progress()
	if progress.Ready != 0 || progress.Pending != 1 || progress.Running != 1 || progress.Blocked != 1 || progress.Remaining != 2 {
		t.Fatalf("running progress = %#v, want ready=0 pending=1 running=1 blocked=1 remaining=2", progress)
	}
	if err := graph.Complete("root", claimed[0].Attempt); err != nil {
		t.Fatalf("Complete(root) error = %v", err)
	}
	progress = graph.Progress()
	if progress.Completed != 1 || progress.Ready != 1 || progress.Pending != 1 || progress.Blocked != 0 || progress.Remaining != 1 {
		t.Fatalf("unblocked progress = %#v, want completed=1 ready=1 pending=1 blocked=0 remaining=1", progress)
	}
	claimed = graph.ClaimReady(0)
	if len(claimed) != 1 || claimed[0].ID != "child" {
		t.Fatalf("ClaimReady(child) = %#v, want child", claimed)
	}
	if err := graph.Complete("child", claimed[0].Attempt); err != nil {
		t.Fatalf("Complete(child) error = %v", err)
	}
	progress = graph.Progress()
	if progress.Completed != 2 || progress.Remaining != 0 || progress.Pending != 0 || progress.Running != 0 {
		t.Fatalf("completed progress = %#v, want completed=2 remaining=0", progress)
	}
}

func TestC238MutationDependencyQueueProgressEstimatesRemainingTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 8)
	if err != nil {
		t.Fatalf("OpenSQLMutationDependencyQueue() error = %v", err)
	}
	defer queue.Close()
	now := time.Unix(1_700_000_000, 0).UTC()
	queue.now = func() time.Time { return now }
	queue.startedAt = now
	for _, id := range []string{"a", "b", "c", "d"} {
		if err := queue.Add(SQLMutationTask{ID: id}); err != nil {
			t.Fatalf("Add(%q) error = %v", id, err)
		}
	}
	now = now.Add(20 * time.Second)
	claimed, err := queue.ClaimReady(1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimReady() = %#v/%v, want one task", claimed, err)
	}
	if err := queue.Complete(claimed[0].ID, claimed[0].Attempt); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	progress := queue.Progress()
	if progress.ElapsedNanos != int64(20*time.Second) {
		t.Fatalf("elapsed nanos = %d, want %d", progress.ElapsedNanos, int64(20*time.Second))
	}
	if progress.Completed != 1 || progress.Remaining != 3 || progress.EstimatedRemainingNanos != int64(60*time.Second) {
		t.Fatalf("timed progress = %#v, want completed=1 remaining=3 estimate=60s", progress)
	}
	encoded, err := json.Marshal(progress)
	if err != nil {
		t.Fatalf("json.Marshal(progress) error = %v", err)
	}
	for _, token := range []string{"\"remaining\":3", "\"elapsed_nanos\":20000000000", "\"estimated_remaining_nanos\":60000000000"} {
		if !containsJSONToken(encoded, token) {
			t.Fatalf("progress JSON = %s, missing %s", encoded, token)
		}
	}
}

func containsJSONToken(data []byte, token string) bool {
	for index := 0; index+len(token) <= len(data); index++ {
		if string(data[index:index+len(token)]) == token {
			return true
		}
	}
	return false
}
