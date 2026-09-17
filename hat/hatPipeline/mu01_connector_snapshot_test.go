package hatPipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestMU01ConnectorRegistrySnapshotRoundTrip(t *testing.T) {
	sourceConnector := &lifecycleTestConnector{}
	source, err := NewConnectorRegistry(ConnectorRegistryOptions{HistoryLimit: 3})
	if err != nil {
		t.Fatalf("NewConnectorRegistry() error = %v", err)
	}
	if err := source.Register("orders", sourceConnector); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	ctx := context.Background()
	for _, operation := range []func(context.Context, string) error{
		source.Start,
		source.Pause,
		source.Resume,
		source.Pause,
		source.Resume,
	} {
		if err := operation(ctx, "orders"); err != nil {
			t.Fatalf("lifecycle operation error = %v", err)
		}
	}

	wantStatus, ok := source.Status("orders")
	if !ok {
		t.Fatal("source status is missing")
	}
	wantEvents := source.Events("orders")
	payload, err := source.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("MarshalSnapshot() returned empty payload")
	}
	secondPayload, err := source.MarshalSnapshot()
	if err != nil {
		t.Fatalf("second MarshalSnapshot() error = %v", err)
	}
	if !bytes.Equal(payload, secondPayload) {
		t.Fatal("MarshalSnapshot() is not deterministic")
	}
	jsonPayload, err := json.Marshal(source.SnapshotState())
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	t.Logf("snapshot wire bytes: binary=%d json=%d", len(payload), len(jsonPayload))

	snapshot, err := UnmarshalConnectorRegistrySnapshot(payload)
	if err != nil {
		t.Fatalf("UnmarshalConnectorRegistrySnapshot() error = %v", err)
	}
	if snapshot.HistoryLimit != 3 || len(snapshot.Connectors) != 1 {
		t.Fatalf("decoded snapshot = %#v", snapshot)
	}

	restoredConnector := &lifecycleTestConnector{}
	restored, err := NewConnectorRegistryFromSnapshot(snapshot, map[string]Connector{
		"orders": restoredConnector,
	})
	if err != nil {
		t.Fatalf("NewConnectorRegistryFromSnapshot() error = %v", err)
	}
	gotStatus, ok := restored.Status("orders")
	if !ok {
		t.Fatal("restored status is missing")
	}
	if !reflect.DeepEqual(gotStatus, wantStatus) {
		t.Fatalf("restored status = %#v, want %#v", gotStatus, wantStatus)
	}
	if gotEvents := restored.Events("orders"); !reflect.DeepEqual(gotEvents, wantEvents) {
		t.Fatalf("restored events = %#v, want %#v", gotEvents, wantEvents)
	}
	if calls := restoredConnector.callNames(); len(calls) != 0 {
		t.Fatalf("restore invoked connector callbacks: %v", calls)
	}

	snapshot.Connectors[0].Events[0].Error = "mutated"
	if restored.Events("orders")[0].Error == "mutated" {
		t.Fatal("restored events share decoded snapshot storage")
	}
	if err := restored.Pause(context.Background(), "orders"); err != nil {
		t.Fatalf("Pause(after restore) error = %v", err)
	}
	if err := restored.Resume(context.Background(), "orders"); err != nil {
		t.Fatalf("Resume(after restore) error = %v", err)
	}
	if events := restored.Events("orders"); len(events) != 3 || events[2].To != ConnectorRunning {
		t.Fatalf("post-restore event ring = %#v", events)
	}
}

func TestMU01ConnectorRegistrySnapshotRejectsInvalidInputWithoutMutation(t *testing.T) {
	source, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatalf("NewConnectorRegistry() error = %v", err)
	}
	if err := source.Register("orders", &lifecycleTestConnector{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := source.Start(context.Background(), "orders"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	payload, err := source.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}

	truncated := payload[:len(payload)-1]
	if _, err := UnmarshalConnectorRegistrySnapshot(truncated); !errors.Is(err, ErrConnectorSnapshotInvalid) {
		t.Fatalf("truncated snapshot error = %v, want %v", err, ErrConnectorSnapshotInvalid)
	}
	corrupted := append([]byte(nil), payload...)
	corrupted[len(corrupted)/2] ^= 1
	if _, err := UnmarshalConnectorRegistrySnapshot(corrupted); !errors.Is(err, ErrConnectorSnapshotInvalid) {
		t.Fatalf("corrupted snapshot error = %v, want %v", err, ErrConnectorSnapshotInvalid)
	}

	clean, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatalf("NewConnectorRegistry(clean) error = %v", err)
	}
	if err := clean.RestoreSnapshot(payload, nil); !errors.Is(err, ErrConnectorSnapshotConnectorMismatch) {
		t.Fatalf("missing connector error = %v, want %v", err, ErrConnectorSnapshotConnectorMismatch)
	}
	if statuses := clean.Snapshot(); len(statuses) != 0 {
		t.Fatalf("clean registry changed after rejected restore: %#v", statuses)
	}

	occupied, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatalf("NewConnectorRegistry(occupied) error = %v", err)
	}
	if err := occupied.Register("existing", &lifecycleTestConnector{}); err != nil {
		t.Fatalf("Register(existing) error = %v", err)
	}
	if err := occupied.RestoreSnapshot(payload, map[string]Connector{
		"orders": &lifecycleTestConnector{},
	}); !errors.Is(err, ErrConnectorSnapshotNotEmpty) {
		t.Fatalf("occupied restore error = %v, want %v", err, ErrConnectorSnapshotNotEmpty)
	}
	if statuses := occupied.Snapshot(); len(statuses) != 1 || statuses[0].ID != "existing" {
		t.Fatalf("occupied registry changed after rejected restore: %#v", statuses)
	}

	invalid := ConnectorRegistrySnapshot{
		HistoryLimit: 1,
		Connectors: []ConnectorSnapshot{
			{Status: ConnectorStatus{ID: "duplicate", State: ConnectorCreated}},
			{Status: ConnectorStatus{ID: "duplicate", State: ConnectorCreated}},
		},
	}
	if _, err := NewConnectorRegistryFromSnapshot(invalid, map[string]Connector{
		"duplicate": &lifecycleTestConnector{},
	}); !errors.Is(err, ErrConnectorSnapshotInvalid) {
		t.Fatalf("duplicate snapshot error = %v, want %v", err, ErrConnectorSnapshotInvalid)
	}
}
