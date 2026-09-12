package hatPipeline

import (
	"bytes"
	"errors"
	"testing"
)

func TestFrontierRegistrySnapshotRoundTrip(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 4})
	if err != nil {
		t.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	for _, id := range []string{"zeta", "alpha"} {
		if err := registry.Register(id); err != nil {
			t.Fatalf("Register(%q) error = %v", id, err)
		}
	}
	if err := registry.Advance("zeta", 12, 15); err != nil {
		t.Fatalf("Advance(zeta) error = %v", err)
	}
	if err := registry.Advance("alpha", 4, 8); err != nil {
		t.Fatalf("Advance(alpha) error = %v", err)
	}

	encoded, err := registry.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("MarshalSnapshot() returned empty data")
	}
	if !bytes.Equal(encoded, mustMarshalFrontierSnapshot(t, registry)) {
		t.Fatal("MarshalSnapshot() is not deterministic")
	}

	restored, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 4})
	if err != nil {
		t.Fatalf("NewFrontierRegistry(restored) error = %v", err)
	}
	if err := restored.RestoreSnapshot(encoded); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	got := restored.SnapshotAll()
	if len(got) != 2 || got[0].ID != "alpha" || got[1].ID != "zeta" {
		t.Fatalf("restored snapshots = %#v", got)
	}
	if got[0].Lower != 4 || got[0].Upper != 8 || got[0].Generation != 1 || got[1].Lower != 12 || got[1].Upper != 15 || got[1].Generation != 1 {
		t.Fatalf("restored frontier values = %#v", got)
	}
	if err := restored.Advance("alpha", 5, 9); err != nil {
		t.Fatalf("Advance(after restore) error = %v", err)
	}
	if err := restored.Advance("alpha", 4, 9); !errors.Is(err, ErrFrontierRegression) {
		t.Fatalf("regression after restore error = %v", err)
	}
}

func TestFrontierRegistryRestoreValidatesBeforeMutation(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 2})
	if err != nil {
		t.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	if err := registry.Register("existing"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	for _, payload := range [][]byte{
		nil,
		[]byte("HTFR\x02"),
		[]byte("HTFR\x01\x01\x01"),
	} {
		if err := registry.RestoreSnapshot(payload); !errors.Is(err, ErrFrontierSnapshotInvalid) {
			t.Fatalf("RestoreSnapshot(%q) error = %v, want %v", payload, err, ErrFrontierSnapshotInvalid)
		}
	}
	if err := registry.RestoreSnapshot(mustMarshalFrontierSnapshot(t, registry)); !errors.Is(err, ErrFrontierSnapshotNotEmpty) {
		t.Fatalf("RestoreSnapshot(non-empty) error = %v, want %v", err, ErrFrontierSnapshotNotEmpty)
	}
	if _, ok := registry.Snapshot("existing"); !ok {
		t.Fatal("existing frontier was changed by rejected restore")
	}
}

func TestFrontierRegistryRestoreRejectsCapacityAndDuplicateIDs(t *testing.T) {
	source, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 2})
	if err != nil {
		t.Fatalf("NewFrontierRegistry(source) error = %v", err)
	}
	for _, id := range []string{"one", "two"} {
		if err := source.Register(id); err != nil {
			t.Fatalf("Register(%q) error = %v", id, err)
		}
	}
	encoded, err := source.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	limited, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 1})
	if err != nil {
		t.Fatalf("NewFrontierRegistry(limited) error = %v", err)
	}
	if err := limited.RestoreSnapshot(encoded); !errors.Is(err, ErrFrontierObjectLimit) {
		t.Fatalf("RestoreSnapshot(capacity) error = %v, want %v", err, ErrFrontierObjectLimit)
	}

	duplicate := append([]byte(nil), encoded...)
	if err := rewriteFrontierSnapshotSecondID(duplicate, "one"); err != nil {
		t.Fatalf("rewrite duplicate snapshot error = %v", err)
	}
	empty, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 2})
	if err != nil {
		t.Fatalf("NewFrontierRegistry(empty) error = %v", err)
	}
	if err := empty.RestoreSnapshot(duplicate); !errors.Is(err, ErrFrontierSnapshotInvalid) {
		t.Fatalf("RestoreSnapshot(duplicate) error = %v, want %v", err, ErrFrontierSnapshotInvalid)
	}
}

func mustMarshalFrontierSnapshot(t *testing.T, registry *FrontierRegistry) []byte {
	t.Helper()
	encoded, err := registry.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	return encoded
}

func rewriteFrontierSnapshotSecondID(payload []byte, id string) error {
	if len(payload) < frontierSnapshotHeaderSize {
		return ErrFrontierSnapshotInvalid
	}
	offset := frontierSnapshotHeaderSize
	_, size := readFrontierSnapshotUvarint(payload[offset:])
	if size <= 0 {
		return ErrFrontierSnapshotInvalid
	}
	offset += size
	firstLength, size := readFrontierSnapshotUvarint(payload[offset:])
	if size <= 0 {
		return ErrFrontierSnapshotInvalid
	}
	offset += size + int(firstLength)
	for field := 0; field < 4; field++ {
		_, size = readFrontierSnapshotUvarint(payload[offset:])
		if size <= 0 {
			return ErrFrontierSnapshotInvalid
		}
		offset += size
	}
	secondLength, size := readFrontierSnapshotUvarint(payload[offset:])
	if size <= 0 || int(secondLength) != len(id) {
		return ErrFrontierSnapshotInvalid
	}
	copy(payload[offset+size:offset+size+int(secondLength)], id)
	return nil
}
