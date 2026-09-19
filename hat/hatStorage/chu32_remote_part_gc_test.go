package hatStorage_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

var chu32DeleteErr = errors.New("delete failed")

type chu32DeleteStore struct {
	deleted []string
	failAt  int
}

func (store *chu32DeleteStore) DeleteRemotePart(_ context.Context, objectURI string) error {
	store.deleted = append(store.deleted, objectURI)
	if store.failAt > 0 && len(store.deleted) == store.failAt {
		return chu32DeleteErr
	}
	return nil
}

func chu32Reference(t *testing.T, name string, size uint64) hatStorage.RemotePartReference {
	t.Helper()
	reference, err := hatStorage.NewRemotePartReference(
		"s3://bucket/parts/"+name,
		"parts/"+name+".json",
		"sha256:"+name,
		size,
	)
	if err != nil {
		t.Fatalf("NewRemotePartReference(%q) error = %v", name, err)
	}
	return reference
}

func TestCHU32PlansOnlyUnreachableObjectsPastRetention(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	live := chu32Reference(t, "live", 10)
	plan, err := hatStorage.PlanRemotePartGarbageCollection(
		[]hatStorage.RemotePartReference{live},
		[]hatStorage.RemotePartGCObject{
			{ObjectURI: live.ObjectURI(), SizeBytes: 10, LastModified: now.Add(-72 * time.Hour)},
			{ObjectURI: "s3://bucket/parts/stale", SizeBytes: 20, LastModified: now.Add(-48 * time.Hour)},
			{ObjectURI: "s3://bucket/parts/young", SizeBytes: 30, LastModified: now.Add(-time.Hour)},
		},
		hatStorage.RemotePartGCOptions{Now: now, MinAge: 24 * time.Hour},
	)
	if err != nil {
		t.Fatalf("PlanRemotePartGarbageCollection() error = %v", err)
	}
	if len(plan.Candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(plan.Candidates))
	}
	if got := plan.Candidates[0].ObjectURI; got != "s3://bucket/parts/stale" {
		t.Fatalf("candidate URI = %q, want stale object", got)
	}
	if plan.ReclaimableBytes != 20 {
		t.Fatalf("reclaimable bytes = %d, want 20", plan.ReclaimableBytes)
	}
}

func TestCHU32PlanRejectsInvalidAndConflictingObjects(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		objects []hatStorage.RemotePartGCObject
	}{
		{
			name: "invalid uri",
			objects: []hatStorage.RemotePartGCObject{{
				ObjectURI:    "file:///unsafe",
				LastModified: now.Add(-48 * time.Hour),
			}},
		},
		{
			name: "malformed host",
			objects: []hatStorage.RemotePartGCObject{{
				ObjectURI:    "http://[bad/parts/object",
				LastModified: now.Add(-48 * time.Hour),
			}},
		},
		{
			name: "conflicting duplicate",
			objects: []hatStorage.RemotePartGCObject{
				{ObjectURI: "s3://bucket/parts/duplicate", SizeBytes: 1, LastModified: now.Add(-48 * time.Hour)},
				{ObjectURI: "s3://bucket/parts/duplicate", SizeBytes: 2, LastModified: now.Add(-48 * time.Hour)},
			},
		},
		{
			name: "missing last modified",
			objects: []hatStorage.RemotePartGCObject{{
				ObjectURI: "s3://bucket/parts/unknown-age",
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := hatStorage.PlanRemotePartGarbageCollection(nil, test.objects, hatStorage.RemotePartGCOptions{Now: now})
			if !errors.Is(err, hatStorage.ErrRemotePartGCInvalid) {
				t.Fatalf("PlanRemotePartGarbageCollection() error = %v, want ErrRemotePartGCInvalid", err)
			}
		})
	}
}

func TestCHU32ExecutionHonorsCancellationAndReportsPartialFailure(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	plan, err := hatStorage.PlanRemotePartGarbageCollection(nil, []hatStorage.RemotePartGCObject{
		{ObjectURI: "s3://bucket/parts/a", SizeBytes: 3, LastModified: now.Add(-48 * time.Hour)},
		{ObjectURI: "s3://bucket/parts/b", SizeBytes: 5, LastModified: now.Add(-48 * time.Hour)},
		{ObjectURI: "s3://bucket/parts/c", SizeBytes: 7, LastModified: now.Add(-48 * time.Hour)},
	}, hatStorage.RemotePartGCOptions{Now: now})
	if err != nil {
		t.Fatalf("PlanRemotePartGarbageCollection() error = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	store := &chu32DeleteStore{}
	if _, err := hatStorage.ExecuteRemotePartGarbageCollection(canceled, store, plan); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ExecuteRemotePartGarbageCollection() error = %v, want context.Canceled", err)
	}
	if len(store.deleted) != 0 {
		t.Fatalf("canceled deletion calls = %d, want 0", len(store.deleted))
	}

	store = &chu32DeleteStore{failAt: 2}
	result, err := hatStorage.ExecuteRemotePartGarbageCollection(context.Background(), store, plan)
	if !errors.Is(err, chu32DeleteErr) {
		t.Fatalf("failed ExecuteRemotePartGarbageCollection() error = %v, want delete error", err)
	}
	if result.DeletedObjects != 1 || result.DeletedBytes != 3 {
		t.Fatalf("partial result = %#v, want one deleted object and 3 bytes", result)
	}
	if result.FailedObjectURI != "s3://bucket/parts/b" {
		t.Fatalf("failed object URI = %q, want b", result.FailedObjectURI)
	}
}

func TestCHU32ExecutionRejectsMutatedPlanBeforeNetwork(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	plan, err := hatStorage.PlanRemotePartGarbageCollection(nil, []hatStorage.RemotePartGCObject{
		{ObjectURI: "s3://bucket/parts/a", SizeBytes: 3, LastModified: now.Add(-48 * time.Hour)},
	}, hatStorage.RemotePartGCOptions{Now: now})
	if err != nil {
		t.Fatalf("PlanRemotePartGarbageCollection() error = %v", err)
	}
	plan.Candidates[0].ObjectURI = "file:///unsafe"

	store := &chu32DeleteStore{}
	if _, err := hatStorage.ExecuteRemotePartGarbageCollection(context.Background(), store, plan); !errors.Is(err, hatStorage.ErrRemotePartGCInvalid) {
		t.Fatalf("mutated plan error = %v, want ErrRemotePartGCInvalid", err)
	}
	if len(store.deleted) != 0 {
		t.Fatalf("mutated plan deletion calls = %d, want 0", len(store.deleted))
	}
}

func TestCHU32PlanBoundsCandidateCount(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	objects := make([]hatStorage.RemotePartGCObject, hatStorage.MaxRemotePartGCCandidates+1)
	for index := range objects {
		objects[index] = hatStorage.RemotePartGCObject{
			ObjectURI:    fmt.Sprintf("s3://bucket/parts/%d", index),
			LastModified: now.Add(-48 * time.Hour),
		}
	}
	_, err := hatStorage.PlanRemotePartGarbageCollection(nil, objects, hatStorage.RemotePartGCOptions{Now: now})
	if !errors.Is(err, hatStorage.ErrRemotePartGCInvalid) {
		t.Fatalf("over-limit plan error = %v, want ErrRemotePartGCInvalid", err)
	}
}
