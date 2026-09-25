package hatSql

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSQLMutationDependencyQueueReplaysDurableTransitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 8)
	if err != nil {
		t.Fatalf("OpenSQLMutationDependencyQueue() error = %v", err)
	}
	for _, task := range []SQLMutationTask{
		{ID: "load"},
		{ID: "index", DependsOn: []string{"load"}},
	} {
		if err := queue.Add(task); err != nil {
			t.Fatalf("Add(%q) error = %v", task.ID, err)
		}
	}
	claimed, err := queue.ClaimReady(0)
	if err != nil {
		t.Fatalf("ClaimReady(load) error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "load" || claimed[0].Attempt != 1 {
		t.Fatalf("ClaimReady(load) = %#v, want load attempt 1", claimed)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	queue, err = OpenSQLMutationDependencyQueue(path, 8)
	if err != nil {
		t.Fatalf("reopen queue error = %v", err)
	}
	load, ok := queue.Task("load")
	if !ok || load.State != SQLMutationTaskRunning || load.Attempt != 1 {
		t.Fatalf("reopened load = %#v/%v, want running attempt 1", load, ok)
	}
	requeued, err := queue.RequeueRunning()
	if err != nil || requeued != 1 {
		t.Fatalf("RequeueRunning() = %d/%v, want 1/nil", requeued, err)
	}
	claimed, err = queue.ClaimReady(1)
	if err != nil || len(claimed) != 1 || claimed[0].Attempt != 2 {
		t.Fatalf("reclaimed load = %#v/%v, want attempt 2", claimed, err)
	}
	if err := queue.Complete("load", claimed[0].Attempt); err != nil {
		t.Fatalf("Complete(load) error = %v", err)
	}
	claimed, err = queue.ClaimReady(0)
	if err != nil || len(claimed) != 1 || claimed[0].ID != "index" {
		t.Fatalf("ClaimReady(index) = %#v/%v, want index", claimed, err)
	}
	if err := queue.Fail("index", claimed[0].Attempt, "temporary"); err != nil {
		t.Fatalf("Fail(index) error = %v", err)
	}
	if err := queue.Retry("index"); err != nil {
		t.Fatalf("Retry(index) error = %v", err)
	}
	claimed, err = queue.ClaimReady(0)
	if err != nil || len(claimed) != 1 || claimed[0].Attempt != 2 {
		t.Fatalf("reclaimed index = %#v/%v, want attempt 2", claimed, err)
	}
	if err := queue.Complete("index", claimed[0].Attempt); err != nil {
		t.Fatalf("Complete(index) error = %v", err)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	queue, err = OpenSQLMutationDependencyQueue(path, 8)
	if err != nil {
		t.Fatalf("final reopen queue error = %v", err)
	}
	defer queue.Close()
	got := queue.Snapshot()
	want := []SQLMutationTaskRecord{
		{ID: "index", DependsOn: []string{"load"}, State: SQLMutationTaskCompleted, Attempt: 2},
		{ID: "load", State: SQLMutationTaskCompleted, Attempt: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("final Snapshot() = %#v, want %#v", got, want)
	}
}

func TestSQLMutationDependencyQueueTruncatesIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 4)
	if err != nil {
		t.Fatalf("open queue error = %v", err)
	}
	if err := queue.Add(SQLMutationTask{ID: "load"}); err != nil {
		t.Fatalf("Add(load) error = %v", err)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open append log error = %v", err)
	}
	if _, err := file.Write([]byte{0x48, 0x4d, 0x51}); err != nil {
		t.Fatalf("write incomplete tail error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close append log error = %v", err)
	}

	queue, err = OpenSQLMutationDependencyQueue(path, 4)
	if err != nil {
		t.Fatalf("reopen incomplete log error = %v", err)
	}
	defer queue.Close()
	if got, ok := queue.Task("load"); !ok || got.State != SQLMutationTaskPending {
		t.Fatalf("reopened load = %#v/%v, want pending", got, ok)
	}
	truncated, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() after replay error = %v", err)
	}
	if truncated.Size() != info.Size() {
		t.Fatalf("replayed log size = %d, want %d after incomplete-tail truncation", truncated.Size(), info.Size())
	}
}

func TestSQLMutationDependencyQueueRejectsCorruptRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 4)
	if err != nil {
		t.Fatalf("open queue error = %v", err)
	}
	if err := queue.Add(SQLMutationTask{ID: "load"}); err != nil {
		t.Fatalf("Add(load) error = %v", err)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	data[len(data)-1] ^= 0x80
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := OpenSQLMutationDependencyQueue(path, 4); !errors.Is(err, ErrSQLMutationDependencyQueueCorrupt) {
		t.Fatalf("open corrupt log error = %v, want ErrSQLMutationDependencyQueueCorrupt", err)
	}
}

func TestSQLMutationDependencyQueueClosedOperations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 1)
	if err != nil {
		t.Fatalf("open queue error = %v", err)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := queue.Add(SQLMutationTask{ID: "load"}); !errors.Is(err, ErrSQLMutationDependencyQueueClosed) {
		t.Fatalf("Add() after close error = %v, want closed", err)
	}
}

func TestSQLMutationDependencyQueueExclusiveLease(t *testing.T) {
	const helperEnvironment = "HATRIE_SQL_MUTATION_QUEUE_LEASE_HELPER"
	if os.Getenv(helperEnvironment) == "1" {
		path := os.Getenv("HATRIE_SQL_MUTATION_QUEUE_LEASE_PATH")
		queue, err := OpenSQLMutationDependencyQueueWithOptions(path, 4, SQLMutationDependencyQueueOptions{ExclusiveLease: true})
		if !errors.Is(err, ErrSQLMutationDependencyQueueLeaseHeld) {
			t.Fatalf("child lease acquisition error = %v, want ErrSQLMutationDependencyQueueLeaseHeld", err)
		}
		if queue != nil {
			t.Fatal("child lease acquisition returned a queue while lease was held")
		}
		return
	}

	path := filepath.Join(t.TempDir(), "mutations.log")
	options := SQLMutationDependencyQueueOptions{ExclusiveLease: true}
	queue, err := OpenSQLMutationDependencyQueueWithOptions(path, 4, options)
	if errors.Is(err, ErrSQLMutationDependencyQueueLeaseUnsupported) {
		t.Skipf("exclusive queue leases are unsupported: %v", err)
	}
	if err != nil {
		t.Fatalf("open leased queue error = %v", err)
	}

	child := exec.Command(os.Args[0], "-test.run=^TestSQLMutationDependencyQueueExclusiveLease$", "-test.count=1")
	child.Env = append(os.Environ(),
		helperEnvironment+"=1",
		"HATRIE_SQL_MUTATION_QUEUE_LEASE_PATH="+path,
	)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("child lease contention error = %v, output = %s", err, output)
	}

	if err := queue.Close(); err != nil {
		t.Fatalf("close leased queue error = %v", err)
	}
	reopened, err := OpenSQLMutationDependencyQueueWithOptions(path, 4, options)
	if err != nil {
		t.Fatalf("reopen leased queue after release error = %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened leased queue error = %v", err)
	}
}

func TestSQLMutationDependencyQueueCompactsWithoutChangingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 16)
	if err != nil {
		t.Fatalf("open queue error = %v", err)
	}
	for index := 0; index < 8; index++ {
		if err := queue.Add(SQLMutationTask{ID: "task-" + string(rune('a'+index))}); err != nil {
			t.Fatalf("Add(task %d) error = %v", index, err)
		}
	}
	for index := 0; index < 8; index++ {
		claimed, claimErr := queue.ClaimReady(1)
		if claimErr != nil || len(claimed) != 1 {
			t.Fatalf("ClaimReady(%d) = %#v/%v, want one task", index, claimed, claimErr)
		}
		if _, requeueErr := queue.RequeueRunning(); requeueErr != nil {
			t.Fatalf("RequeueRunning(%d) error = %v", index, requeueErr)
		}
	}
	before := queue.Snapshot()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(before compact) error = %v", err)
	}
	if err := queue.Compact(); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(after compact) error = %v", err)
	}
	if after.Size() >= info.Size() {
		t.Fatalf("compacted log size = %d, want less than %d", after.Size(), info.Size())
	}
	if got := queue.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after Compact() = %#v, want %#v", got, before)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	queue, err = OpenSQLMutationDependencyQueue(path, 16)
	if err != nil {
		t.Fatalf("reopen compacted queue error = %v", err)
	}
	defer queue.Close()
	if got := queue.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("reopened compacted state = %#v, want %#v", got, before)
	}
}
