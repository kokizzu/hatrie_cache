package hatPipeline

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

type connectorCheckpointMemoryStore struct {
	payload []byte
	loadErr error
	saveErr error
}

func (s *connectorCheckpointMemoryStore) Load(ctx context.Context) ([]byte, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return append([]byte(nil), s.payload...), nil
}

func (s *connectorCheckpointMemoryStore) Save(ctx context.Context, payload []byte) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if s.saveErr != nil {
		return s.saveErr
	}
	s.payload = append(s.payload[:0], payload...)
	return nil
}

func newRunningConnectorCheckpointRegistry(t *testing.T) (*ConnectorRegistry, *lifecycleTestConnector) {
	t.Helper()
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	connector := &lifecycleTestConnector{}
	if err := registry.Register("orders", connector); err != nil {
		t.Fatal(err)
	}
	if err := registry.Start(context.Background(), "orders"); err != nil {
		t.Fatal(err)
	}
	return registry, connector
}

func TestConnectorCheckpointRoundTripsAndRestoresBeforeResume(t *testing.T) {
	registry, connector := newRunningConnectorCheckpointRegistry(t)
	store := &connectorCheckpointMemoryStore{}
	offset := []byte("offset-v1")
	frontier := []byte("frontier-v1")

	checkpoint, err := registry.PauseWithCheckpoint(context.Background(), "orders", ConnectorCheckpoint{
		Sequence: 7,
		Offset:   offset,
		Frontier: frontier,
	}, store)
	if err != nil {
		t.Fatalf("PauseWithCheckpoint() error = %v", err)
	}
	if checkpoint.ConnectorID != "orders" {
		t.Fatalf("checkpoint connector ID = %q, want orders", checkpoint.ConnectorID)
	}
	status, ok := registry.Status("orders")
	if !ok {
		t.Fatal("connector status missing after checkpoint")
	}
	if status.State != ConnectorPaused {
		t.Fatalf("state after checkpoint = %v, want paused", status.State)
	}
	if checkpoint.Generation != status.Generation {
		t.Fatalf("checkpoint generation = %d, status generation = %d", checkpoint.Generation, status.Generation)
	}
	offset[0] = 'X'
	frontier[0] = 'Y'
	if string(checkpoint.Offset) != "offset-v1" || string(checkpoint.Frontier) != "frontier-v1" {
		t.Fatal("returned checkpoint aliases caller-owned data")
	}

	persisted, err := DecodeConnectorCheckpoint(store.payload)
	if err != nil {
		t.Fatalf("DecodeConnectorCheckpoint() error = %v", err)
	}
	if !reflect.DeepEqual(persisted, checkpoint) {
		t.Fatalf("persisted checkpoint = %#v, want %#v", persisted, checkpoint)
	}

	resumed, err := registry.ResumeFromCheckpoint(context.Background(), "orders", store, func(_ context.Context, restored ConnectorCheckpoint) error {
		if string(restored.Offset) != "offset-v1" || string(restored.Frontier) != "frontier-v1" {
			t.Fatalf("restored checkpoint = %#v", restored)
		}
		connector.mu.Lock()
		connector.calls = append(connector.calls, "restore")
		connector.mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("ResumeFromCheckpoint() error = %v", err)
	}
	if !reflect.DeepEqual(resumed, checkpoint) {
		t.Fatalf("resumed checkpoint = %#v, want %#v", resumed, checkpoint)
	}
	status, ok = registry.Status("orders")
	if !ok || status.State != ConnectorRunning {
		t.Fatalf("state after resume = %#v, want running", status)
	}
	if got, want := connector.callNames(), []string{"start", "pause", "restore", "resume"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("connector calls = %#v, want %#v", got, want)
	}
	if events := registry.Events("orders"); len(events) != 3 || events[1].To != ConnectorPaused || events[2].To != ConnectorRunning {
		t.Fatalf("lifecycle audit events = %#v", events)
	}
}

func TestConnectorCheckpointRejectsStaleGenerationWithoutApplying(t *testing.T) {
	registry, _ := newRunningConnectorCheckpointRegistry(t)
	store := &connectorCheckpointMemoryStore{}
	checkpoint, err := registry.PauseWithCheckpoint(context.Background(), "orders", ConnectorCheckpoint{Sequence: 1, Offset: []byte("offset")}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Resume(context.Background(), "orders"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Pause(context.Background(), "orders"); err != nil {
		t.Fatal(err)
	}

	applied := false
	if _, err := registry.ResumeFromCheckpoint(context.Background(), "orders", store, func(context.Context, ConnectorCheckpoint) error {
		applied = true
		return nil
	}); !errors.Is(err, ErrConnectorCheckpointStale) {
		t.Fatalf("ResumeFromCheckpoint() error = %v, want stale error", err)
	}
	if applied {
		t.Fatal("stale checkpoint was applied")
	}
	status, ok := registry.Status("orders")
	if !ok || status.State != ConnectorPaused {
		t.Fatalf("state after stale resume = %#v, want paused", status)
	}
	if status.Generation == checkpoint.Generation {
		t.Fatalf("generation did not advance after lifecycle changes: %#v", status)
	}
}

func TestConnectorCheckpointValidationAndSaveFailure(t *testing.T) {
	registry, _ := newRunningConnectorCheckpointRegistry(t)
	store := &connectorCheckpointMemoryStore{}

	if _, err := registry.PauseWithCheckpoint(context.Background(), "orders", ConnectorCheckpoint{Sequence: 1}, nil); !errors.Is(err, ErrConnectorCheckpointStoreRequired) {
		t.Fatalf("nil store error = %v, want required error", err)
	}
	if _, err := registry.PauseWithCheckpoint(context.Background(), "orders", ConnectorCheckpoint{}, store); !errors.Is(err, ErrConnectorCheckpointSequenceInvalid) {
		t.Fatalf("zero sequence error = %v, want sequence error", err)
	}
	if _, err := registry.PauseWithCheckpoint(context.Background(), "orders", ConnectorCheckpoint{ConnectorID: "other", Sequence: 1}, store); !errors.Is(err, ErrConnectorCheckpointIDMismatch) {
		t.Fatalf("mismatched ID error = %v, want mismatch error", err)
	}
	if _, err := registry.PauseWithCheckpoint(context.Background(), "orders", ConnectorCheckpoint{Sequence: 1, Offset: bytes.Repeat([]byte{'x'}, maxConnectorCheckpointFieldBytes+1)}, store); !errors.Is(err, ErrConnectorCheckpointPayloadTooLarge) {
		t.Fatalf("oversized offset error = %v, want payload error", err)
	}

	saveErr := errors.New("checkpoint save failed")
	store.saveErr = saveErr
	if _, err := registry.PauseWithCheckpoint(context.Background(), "orders", ConnectorCheckpoint{Sequence: 1, Offset: []byte("offset")}, store); !errors.Is(err, saveErr) {
		t.Fatalf("save failure = %v, want %v", err, saveErr)
	}
	status, ok := registry.Status("orders")
	if !ok || status.State != ConnectorPaused {
		t.Fatalf("state after save failure = %#v, want paused", status)
	}
}

func TestConnectorCheckpointResumeRejectsMissingCorruptAndMismatchedPayloads(t *testing.T) {
	registry, _ := newRunningConnectorCheckpointRegistry(t)
	store := &connectorCheckpointMemoryStore{}

	if _, err := registry.ResumeFromCheckpoint(context.Background(), "orders", nil, func(context.Context, ConnectorCheckpoint) error { return nil }); !errors.Is(err, ErrConnectorCheckpointStoreRequired) {
		t.Fatalf("nil store error = %v, want required error", err)
	}
	if _, err := registry.ResumeFromCheckpoint(context.Background(), "orders", store, func(context.Context, ConnectorCheckpoint) error { return nil }); !errors.Is(err, ErrConnectorCheckpointNotFound) {
		t.Fatalf("missing checkpoint error = %v, want not found error", err)
	}

	store.payload = []byte("not a checkpoint")
	if _, err := registry.ResumeFromCheckpoint(context.Background(), "orders", store, func(context.Context, ConnectorCheckpoint) error { return nil }); !errors.Is(err, ErrConnectorCheckpointInvalid) {
		t.Fatalf("corrupt checkpoint error = %v, want invalid error", err)
	}

	payload, err := EncodeConnectorCheckpoint(ConnectorCheckpoint{ConnectorID: "other", Sequence: 1, Generation: 1, Offset: []byte("offset")})
	if err != nil {
		t.Fatal(err)
	}
	store.payload = payload
	if _, err := registry.ResumeFromCheckpoint(context.Background(), "orders", store, func(context.Context, ConnectorCheckpoint) error { return nil }); !errors.Is(err, ErrConnectorCheckpointIDMismatch) {
		t.Fatalf("mismatched checkpoint error = %v, want mismatch error", err)
	}
}

func TestConnectorCheckpointApplierFailureLeavesConnectorPausedForRetry(t *testing.T) {
	registry, _ := newRunningConnectorCheckpointRegistry(t)
	store := &connectorCheckpointMemoryStore{}
	checkpoint, err := registry.PauseWithCheckpoint(context.Background(), "orders", ConnectorCheckpoint{Sequence: 1, Offset: []byte("offset")}, store)
	if err != nil {
		t.Fatal(err)
	}
	applyErr := errors.New("source restore failed")
	if _, err := registry.ResumeFromCheckpoint(context.Background(), "orders", store, func(context.Context, ConnectorCheckpoint) error {
		return applyErr
	}); !errors.Is(err, applyErr) {
		t.Fatalf("restore callback error = %v, want %v", err, applyErr)
	}
	status, ok := registry.Status("orders")
	if !ok || status.State != ConnectorPaused || status.Generation != checkpoint.Generation {
		t.Fatalf("state after failed apply = %#v, want paused at generation %d", status, checkpoint.Generation)
	}
	if _, err := registry.ResumeFromCheckpoint(context.Background(), "orders", store, func(context.Context, ConnectorCheckpoint) error { return nil }); err != nil {
		t.Fatalf("retry ResumeFromCheckpoint() error = %v", err)
	}
}

func TestConnectorCheckpointEncodingIsDeterministicAndChecksIntegrity(t *testing.T) {
	checkpoint := ConnectorCheckpoint{
		ConnectorID: "orders",
		Sequence:    11,
		Generation:  13,
		Offset:      []byte("offset"),
		Frontier:    []byte("frontier"),
	}
	payloadA, err := EncodeConnectorCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	payloadB, err := EncodeConnectorCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payloadA, payloadB) {
		t.Fatal("checkpoint encoding is not deterministic")
	}
	decoded, err := DecodeConnectorCheckpoint(payloadA)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, checkpoint) {
		t.Fatalf("decoded checkpoint = %#v, want %#v", decoded, checkpoint)
	}
	payloadA[len(payloadA)-1]++
	if _, err := DecodeConnectorCheckpoint(payloadA); !errors.Is(err, ErrConnectorCheckpointInvalid) {
		t.Fatalf("checksum error = %v, want invalid error", err)
	}
}

func TestConnectorCheckpointUsesExistingDurableFileStore(t *testing.T) {
	registry, _ := newRunningConnectorCheckpointRegistry(t)
	store, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{
		Path: filepath.Join(t.TempDir(), "orders.checkpoint"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.PauseWithCheckpoint(context.Background(), "orders", ConnectorCheckpoint{Sequence: 1, Frontier: []byte("frontier")}, store); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ResumeFromCheckpoint(context.Background(), "orders", store, func(context.Context, ConnectorCheckpoint) error { return nil }); err != nil {
		t.Fatal(err)
	}
}
