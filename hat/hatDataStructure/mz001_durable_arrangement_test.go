package hatDataStructure_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestMZ01SpillableArrangementReopensDurableSegment(t *testing.T) {
	directory := t.TempDir()
	options := hatDataStructure.SpillableArrangementOptions{
		Directory:        directory,
		MemoryLimitBytes: 1,
		MaxDiskBytes:     1 << 20,
	}
	source, err := hatDataStructure.NewSpillableArrangement(options)
	if err != nil {
		t.Fatalf("NewSpillableArrangement() error = %v", err)
	}
	values := []hatDataStructure.SpillableArrangementEntry{
		{Key: "alpha", Value: []byte("one")},
		{Key: "beta", Value: []byte("two")},
		{Key: "gamma", Value: []byte("three")},
	}
	if err := source.Set("alpha", []byte("stale")); err != nil {
		t.Fatalf("Set(alpha stale) error = %v", err)
	}
	for _, entry := range values {
		if err := source.Set(entry.Key, entry.Value); err != nil {
			t.Fatalf("Set(%q) error = %v", entry.Key, err)
		}
	}
	if err := source.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	want, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	path := source.SpillPath()
	if err := source.Close(); err != nil {
		t.Fatalf("Close(source) error = %v", err)
	}

	reopened, err := hatDataStructure.OpenSpillableArrangement(path, options)
	if err != nil {
		t.Fatalf("OpenSpillableArrangement() error = %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot(reopened) error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reopened snapshot = %#v, want %#v", got, want)
	}
	stats := reopened.Stats()
	if stats.Entries != len(values) || stats.ColdEntries != len(values) || stats.HotBytes != 0 {
		t.Fatalf("reopened stats = %#v, want %d cold entries and no hot bytes", stats, len(values))
	}
	if value, found, err := reopened.Get("beta"); err != nil || !found || string(value) != "two" {
		t.Fatalf("Get(beta) = %q/%t/%v, want two/true/nil", value, found, err)
	}
	if err := reopened.Set("delta", []byte("four")); err != nil {
		t.Fatalf("Set(delta) after reopen error = %v", err)
	}
	if value, found, err := reopened.Get("delta"); err != nil || !found || string(value) != "four" {
		t.Fatalf("Get(delta) = %q/%t/%v, want four/true/nil", value, found, err)
	}
	if err := reopened.Flush(); err != nil {
		t.Fatalf("Flush(reopened) error = %v", err)
	}
	if err := reopened.Compact(); err != nil {
		t.Fatalf("Compact(reopened) error = %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close(reopened) error = %v", err)
	}
	reopened, err = hatDataStructure.OpenSpillableArrangement(path, options)
	if err != nil {
		t.Fatalf("OpenSpillableArrangement(after compact) error = %v", err)
	}
	defer reopened.Close()
	if value, found, err := reopened.Get("delta"); err != nil || !found || string(value) != "four" {
		t.Fatalf("Get(delta after compact) = %q/%t/%v, want four/true/nil", value, found, err)
	}

	corruptPath := filepath.Join(directory, "corrupt-arrangement")
	if err := os.WriteFile(corruptPath, []byte("HSA1"), 0o600); err != nil {
		t.Fatalf("WriteFile(corrupt) error = %v", err)
	}
	if _, err := hatDataStructure.OpenSpillableArrangement(corruptPath, options); !errors.Is(err, hatDataStructure.ErrSpillableArrangementCorrupt) {
		t.Fatalf("OpenSpillableArrangement(corrupt) error = %v, want corrupt error", err)
	}
	symlinkPath := filepath.Join(directory, "symlink-arrangement")
	if err := os.Symlink(path, symlinkPath); err == nil {
		if _, err := hatDataStructure.OpenSpillableArrangement(symlinkPath, options); !errors.Is(err, hatDataStructure.ErrSpillableArrangementPathInvalid) {
			t.Fatalf("OpenSpillableArrangement(symlink) error = %v, want invalid path error", err)
		}
	}
}
