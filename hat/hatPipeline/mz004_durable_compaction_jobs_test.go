package hatPipeline

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
)

type mz004DurableCompactionJobMemoryStore struct {
	payload []byte
}

func (store *mz004DurableCompactionJobMemoryStore) Load(context.Context) ([]byte, error) {
	return append([]byte(nil), store.payload...), nil
}

func (store *mz004DurableCompactionJobMemoryStore) Save(_ context.Context, payload []byte) error {
	store.payload = append(store.payload[:0], payload...)
	return nil
}

func TestMZ004DurableCompactionJobLedgerLifecycleAndPruning(t *testing.T) {
	ledger, err := NewFrontierCompactionJobLedger(FrontierCompactionJobLedgerOptions{MaxJobs: 4})
	if err != nil {
		t.Fatal(err)
	}
	first, err := ledger.Enqueue("orders", 100)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ledger.Enqueue("profiles", 200)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != 1 || second.ID != 2 || first.State != FrontierCompactionJobPending {
		t.Fatalf("jobs = %#v/%#v", first, second)
	}
	if err := ledger.Claim(first.ID); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Complete(first.ID); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Complete(second.ID); !errors.Is(err, ErrFrontierCompactionJobTransition) {
		t.Fatalf("complete pending job error = %v, want transition error", err)
	}
	if err := ledger.Claim(second.ID); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Fail(second.ID); err != nil {
		t.Fatal(err)
	}
	if got := ledger.PruneCompletedThrough(first.ID); got != 1 {
		t.Fatalf("pruned jobs = %d, want 1", got)
	}
	if _, ok := ledger.Job(first.ID); ok {
		t.Fatal("completed job remains after pruning")
	}
	if job, ok := ledger.Job(second.ID); !ok || job.State != FrontierCompactionJobFailed {
		t.Fatalf("failed job = %#v/%v", job, ok)
	}
	if err := ledger.Claim(999); !errors.Is(err, ErrFrontierCompactionJobNotFound) {
		t.Fatalf("unknown claim error = %v, want not found", err)
	}
}

func TestMZ004DurableCompactionJobSnapshotRoundTripsAndRecoversRunning(t *testing.T) {
	ledger, err := NewFrontierCompactionJobLedger(FrontierCompactionJobLedgerOptions{MaxJobs: 8})
	if err != nil {
		t.Fatal(err)
	}
	job, err := ledger.Enqueue("orders", 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Claim(job.ID); err != nil {
		t.Fatal(err)
	}
	payload, err := ledger.MarshalSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) == 0 {
		t.Fatal("empty durable job snapshot")
	}

	restored, err := NewFrontierCompactionJobLedger(FrontierCompactionJobLedgerOptions{MaxJobs: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreSnapshot(payload); err != nil {
		t.Fatal(err)
	}
	recovered, ok := restored.Job(job.ID)
	if !ok || recovered.State != FrontierCompactionJobPending {
		t.Fatalf("recovered running job = %#v/%v, want pending", recovered, ok)
	}
	if got := restored.Recoverable(); len(got) != 1 || got[0].ID != job.ID {
		t.Fatalf("recoverable jobs = %#v, want job %d", got, job.ID)
	}
	if err := restored.RestoreSnapshot(payload); !errors.Is(err, ErrFrontierCompactionJobSnapshotNotEmpty) {
		t.Fatalf("second restore error = %v, want non-empty", err)
	}
	corrupt := append([]byte(nil), payload...)
	corrupt[len(corrupt)-1]++
	empty, err := NewFrontierCompactionJobLedger(FrontierCompactionJobLedgerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := empty.RestoreSnapshot(corrupt); !errors.Is(err, ErrFrontierCompactionJobSnapshotInvalid) {
		t.Fatalf("corrupt restore error = %v, want invalid", err)
	}

	store := &mz004DurableCompactionJobMemoryStore{}
	if err := ledger.SaveDurableSnapshot(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	fromStore, err := NewFrontierCompactionJobLedger(FrontierCompactionJobLedgerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	found, err := fromStore.RestoreDurableSnapshot(context.Background(), store)
	if err != nil || !found {
		t.Fatalf("durable restore = found=%v err=%v", found, err)
	}
	if !reflect.DeepEqual(fromStore.Snapshot(), restored.Snapshot()) {
		t.Fatalf("store snapshot = %#v, want %#v", fromStore.Snapshot(), restored.Snapshot())
	}
	if !bytes.Equal(payload, store.payload) {
		t.Fatal("durable store payload differs from snapshot")
	}
}

func TestMZ004DurableCompactionJobLedgerValidatesBounds(t *testing.T) {
	if _, err := NewFrontierCompactionJobLedger(FrontierCompactionJobLedgerOptions{MaxJobs: -1}); !errors.Is(err, ErrFrontierCompactionJobLedgerOptionsInvalid) {
		t.Fatalf("negative max jobs error = %v", err)
	}
	ledger, err := NewFrontierCompactionJobLedger(FrontierCompactionJobLedgerOptions{MaxJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Enqueue("", 1); !errors.Is(err, ErrFrontierCompactionJobFrontierRequired) {
		t.Fatalf("empty frontier error = %v", err)
	}
	if _, err := ledger.Enqueue("orders", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Enqueue("orders", 2); !errors.Is(err, ErrFrontierCompactionJobLimit) {
		t.Fatalf("job limit error = %v", err)
	}
	if err := ledger.Complete(1); !errors.Is(err, ErrFrontierCompactionJobTransition) {
		t.Fatalf("complete pending error = %v", err)
	}
}

func TestMZ004DurableCompactionJobDurabilityValidatesStoreAndContext(t *testing.T) {
	ledger, err := NewFrontierCompactionJobLedger(FrontierCompactionJobLedgerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.SaveDurableSnapshot(context.Background(), nil); !errors.Is(err, ErrFrontierCompactionJobStoreRequired) {
		t.Fatalf("nil save store error = %v", err)
	}
	if _, err := ledger.RestoreDurableSnapshot(context.Background(), nil); !errors.Is(err, ErrFrontierCompactionJobStoreRequired) {
		t.Fatalf("nil restore store error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	store := &mz004DurableCompactionJobMemoryStore{}
	if err := ledger.SaveDurableSnapshot(canceled, store); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled save error = %v", err)
	}
	if _, err := ledger.RestoreDurableSnapshot(canceled, store); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled restore error = %v", err)
	}
}

func TestMZ004DurableCompactionJobSnapshotRejectsTrailingAndInvalidState(t *testing.T) {
	ledger, err := NewFrontierCompactionJobLedger(FrontierCompactionJobLedgerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job, err := ledger.Enqueue("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := encodeFrontierCompactionJobSnapshot(2, []FrontierCompactionJob{job})
	if err != nil {
		t.Fatal(err)
	}
	trailing := append(append([]byte(nil), payload...), 1)
	if err := (&FrontierCompactionJobLedger{maxJobs: 4}).RestoreSnapshot(trailing); !errors.Is(err, ErrFrontierCompactionJobSnapshotInvalid) {
		t.Fatalf("trailing bytes error = %v", err)
	}
	if _, err := encodeFrontierCompactionJobSnapshot(2, []FrontierCompactionJob{{
		ID:         1,
		FrontierID: "orders",
		Boundary:   1,
		State:      FrontierCompactionJobState(99),
	}}); !errors.Is(err, ErrFrontierCompactionJobSnapshotInvalid) {
		t.Fatalf("invalid state encode error = %v", err)
	}
}
