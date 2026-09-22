//go:build t215

package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

func TestT215StorageSpaceDefaultsToMemtxAndClonesValues(t *testing.T) {
	space, err := NewStorageSpace(StorageSpaceOptions{Name: "hot"})
	if err != nil {
		t.Fatalf("NewStorageSpace() error = %v", err)
	}
	defer space.Close()
	if got := space.Mode(); got != StorageSpaceMemtx {
		t.Fatalf("Mode() = %q, want %q", got, StorageSpaceMemtx)
	}

	value := []byte("one")
	if err := space.Set("b", value); err != nil {
		t.Fatalf("Set(b) error = %v", err)
	}
	value[0] = 'X'
	got, found, err := space.Get("b")
	if err != nil || !found || string(got) != "one" {
		t.Fatalf("Get(b) = %q, %t, %v, want one/true/nil", got, found, err)
	}
	got[0] = 'Y'
	got, found, err = space.Get("b")
	if err != nil || !found || string(got) != "one" {
		t.Fatalf("Get(b) after caller mutation = %q, %t, %v, want one/true/nil", got, found, err)
	}
	if err := space.Set("a", []byte("two")); err != nil {
		t.Fatalf("Set(a) error = %v", err)
	}
	rows, err := space.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	want := []StorageSpaceEntry{{Key: "a", Value: []byte("two")}, {Key: "b", Value: []byte("one")}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", rows, want)
	}
	stats := space.Stats()
	if stats.Name != "hot" || stats.Mode != StorageSpaceMemtx || stats.Entries != 2 || stats.HotBytes != 6 || stats.DiskBytes != 0 {
		t.Fatalf("Stats() = %+v, want memtx 2 entries/6 hot bytes/no disk", stats)
	}
}

func TestT215StorageSpaceOnDiskSurvivesReopen(t *testing.T) {
	space, err := NewStorageSpace(StorageSpaceOptions{
		Name:             "cold",
		Mode:             StorageSpaceOnDisk,
		Directory:        t.TempDir(),
		MemoryLimitBytes: 1,
	})
	if err != nil {
		t.Fatalf("NewStorageSpace(on-disk) error = %v", err)
	}
	if err := space.Set("a", []byte("alpha")); err != nil {
		t.Fatalf("Set(a) error = %v", err)
	}
	if err := space.Set("b", []byte("bravo")); err != nil {
		t.Fatalf("Set(b) error = %v", err)
	}
	if err := space.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	path := space.Path()
	if path == "" {
		t.Fatal("Path() is empty for on-disk space")
	}
	if stats := space.Stats(); stats.Mode != StorageSpaceOnDisk || stats.Entries != 2 || stats.ColdEntries != 2 || stats.DiskBytes == 0 {
		t.Fatalf("on-disk Stats() = %+v, want two cold entries and disk bytes", stats)
	}
	if err := space.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := NewStorageSpace(StorageSpaceOptions{
		Name:             "cold-restored",
		Mode:             StorageSpaceOnDisk,
		SpillPath:        path,
		MemoryLimitBytes: 1,
	})
	if err != nil {
		t.Fatalf("reopen on-disk space error = %v", err)
	}
	defer reopened.Close()
	for key, want := range map[string]string{"a": "alpha", "b": "bravo"} {
		value, found, err := reopened.Get(key)
		if err != nil || !found || string(value) != want {
			t.Fatalf("reopened Get(%q) = %q, %t, %v, want %q/true/nil", key, value, found, err, want)
		}
	}
	removed, err := reopened.Delete("a")
	if err != nil || !removed {
		t.Fatalf("Delete(a) = %t, %v, want true/nil", removed, err)
	}
	removed, err = reopened.Delete("missing")
	if err != nil || removed {
		t.Fatal("Delete() did not report exactly one removal")
	}
	if err := reopened.Compact(); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	rows, err := reopened.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() after delete error = %v", err)
	}
	wantRows := []StorageSpaceEntry{{Key: "b", Value: []byte("bravo")}}
	if !reflect.DeepEqual(rows, wantRows) {
		t.Fatalf("Snapshot() after delete = %#v, want %#v", rows, wantRows)
	}
}

func TestT215StorageSpaceRejectsInvalidPoliciesAndClosedUse(t *testing.T) {
	if _, err := NewStorageSpace(StorageSpaceOptions{}); !errors.Is(err, ErrStorageSpaceNameRequired) {
		t.Fatalf("empty name error = %v, want %v", err, ErrStorageSpaceNameRequired)
	}
	if _, err := NewStorageSpace(StorageSpaceOptions{Name: "bad", Mode: StorageSpaceMode("unknown")}); !errors.Is(err, ErrStorageSpaceModeInvalid) {
		t.Fatalf("unknown mode error = %v, want %v", err, ErrStorageSpaceModeInvalid)
	}
	if _, err := NewStorageSpace(StorageSpaceOptions{Name: "bad", Mode: StorageSpaceMemtx, SpillPath: "path"}); !errors.Is(err, ErrStorageSpacePolicyConflict) {
		t.Fatalf("memtx spill path error = %v, want %v", err, ErrStorageSpacePolicyConflict)
	}
	space, err := NewStorageSpace(StorageSpaceOptions{Name: "closed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Close(); err != nil {
		t.Fatal(err)
	}
	if err := space.Set("key", []byte("value")); !errors.Is(err, ErrStorageSpaceClosed) {
		t.Fatalf("Set() after Close error = %v, want %v", err, ErrStorageSpaceClosed)
	}
}
