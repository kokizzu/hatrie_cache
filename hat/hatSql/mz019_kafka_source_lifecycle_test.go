package hatSql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type mz019CheckpointStore struct {
	snapshot KafkaTableSourceSnapshot
	found    bool
	err      error
}

func (store *mz019CheckpointStore) Load(context.Context, string) (KafkaTableSourceSnapshot, bool, error) {
	if store.err != nil {
		return KafkaTableSourceSnapshot{}, false, store.err
	}
	return store.snapshot, store.found, nil
}

func (store *mz019CheckpointStore) Commit(_ context.Context, snapshot KafkaTableSourceSnapshot) error {
	if store.err != nil {
		return store.err
	}
	store.snapshot = snapshot
	store.found = true
	return nil
}

func mz019NewSource(t *testing.T) *KafkaTableSource {
	t.Helper()
	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source:  "lifecycle",
		Table:   "orders",
		Topic:   "orders",
		Decoder: KafkaTableJSONDecoder,
	})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func TestMZ019KafkaSourceLifecyclePausesAndResumesIngestion(t *testing.T) {
	source := mz019NewSource(t)
	initial := source.Lifecycle()
	if initial.State != KafkaTableSourceLifecycleRunning || initial.Generation != 0 {
		t.Fatalf("initial lifecycle = %#v", initial)
	}
	paused, err := source.Pause("maintenance")
	if err != nil {
		t.Fatal(err)
	}
	if paused.State != KafkaTableSourceLifecyclePaused || paused.Generation != 1 || paused.Reason != "maintenance" {
		t.Fatalf("paused lifecycle = %#v", paused)
	}
	if _, err := source.ApplyBatch(KafkaTableBatch{Messages: []KafkaTableMessage{{Topic: "orders", Partition: "0", Offset: 1, Key: "1", Value: []byte(`{"id":1}`)}}}); !errors.Is(err, ErrKafkaTableSourcePaused) {
		t.Fatalf("paused apply error = %v", err)
	}
	pausedAgain, err := source.Pause("ignored")
	if err != nil || !reflect.DeepEqual(pausedAgain, paused) {
		t.Fatalf("idempotent pause = %#v, err = %v", pausedAgain, err)
	}
	resumed, err := source.Resume()
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != KafkaTableSourceLifecycleRunning || resumed.Generation != 2 || resumed.Reason != "" {
		t.Fatalf("resumed lifecycle = %#v", resumed)
	}
	if _, err := source.ApplyBatch(KafkaTableBatch{Messages: []KafkaTableMessage{{Topic: "orders", Partition: "0", Offset: 1, Key: "1", Value: []byte(`{"id":1}`)}}}); err != nil {
		t.Fatal(err)
	}
}

func TestMZ019KafkaSourceLifecycleRestoresAndValidatesSnapshotState(t *testing.T) {
	source := mz019NewSource(t)
	if _, err := source.Pause("planned restart"); err != nil {
		t.Fatal(err)
	}
	snapshot := source.Snapshot()
	restored := mz019NewSource(t)
	if err := restored.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.Lifecycle(), source.Lifecycle()) {
		t.Fatalf("restored lifecycle = %#v, want %#v", restored.Lifecycle(), source.Lifecycle())
	}
	legacy := snapshot
	legacy.Lifecycle = KafkaTableSourceLifecycle{}
	if err := restored.Restore(legacy); err != nil {
		t.Fatal(err)
	}
	if restored.Lifecycle().State != KafkaTableSourceLifecycleRunning {
		t.Fatalf("legacy lifecycle = %#v", restored.Lifecycle())
	}
	invalid := snapshot
	invalid.Lifecycle.State = "unknown"
	if err := restored.Restore(invalid); !errors.Is(err, ErrKafkaTableSourceSnapshotInvalid) {
		t.Fatalf("invalid lifecycle error = %v", err)
	}
}

func TestMZ019KafkaSourceLifecycleDurablePauseAndResume(t *testing.T) {
	store := &mz019CheckpointStore{}
	source := mz019NewSource(t)
	paused, err := source.PauseWithCheckpoint(context.Background(), store, "operator request")
	if err != nil {
		t.Fatal(err)
	}
	restored := mz019NewSource(t)
	found, err := restored.RestoreCheckpoint(context.Background(), store)
	if err != nil || !found || !reflect.DeepEqual(restored.Lifecycle(), paused) {
		t.Fatalf("durable pause found=%v err=%v lifecycle=%#v want %#v", found, err, restored.Lifecycle(), paused)
	}
	store.err = errors.New("disk full")
	if _, err := restored.ResumeWithCheckpoint(context.Background(), store); !errors.Is(err, ErrKafkaTableSourceCheckpointCommit) {
		t.Fatalf("failed resume error = %v", err)
	}
	if restored.Lifecycle().State != KafkaTableSourceLifecyclePaused {
		t.Fatalf("failed resume lifecycle = %#v", restored.Lifecycle())
	}
}

func TestMZ019KafkaSourceLifecycleRejectsUnsafeReasons(t *testing.T) {
	source := mz019NewSource(t)
	for _, reason := range []string{"\x00", strings.Repeat("x", MaxKafkaTableSourcePauseReasonBytes+1)} {
		if _, err := source.Pause(reason); !errors.Is(err, ErrKafkaTableSourceLifecycleInvalid) {
			t.Fatalf("reason %q error = %v", reason, err)
		}
	}
}
