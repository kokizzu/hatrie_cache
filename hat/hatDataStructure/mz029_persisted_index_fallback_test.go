package hatDataStructure_test

import (
	"os"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestMZ029PersistedIndexCorruptionFallsBackToSegmentScan(t *testing.T) {
	directory := t.TempDir()
	options := hatDataStructure.SpillableArrangementOptions{
		Directory:        directory,
		MemoryLimitBytes: 1,
	}
	arrangement, err := hatDataStructure.NewSpillableArrangement(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Set("key-17", []byte("value-17")); err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Flush(); err != nil {
		t.Fatal(err)
	}
	spillPath := arrangement.SpillPath()
	indexPath := arrangement.SpillIndexPath()
	if err := arrangement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(indexPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := hatDataStructure.OpenSpillableArrangement(spillPath, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, ok, err := reopened.Get("key-17")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected the segment scan fallback to recover key-17")
	}
	if string(value) != "value-17" {
		t.Fatalf("value = %q, want %q", value, "value-17")
	}
}

func TestMZ029PersistedIndexReflectsFlushedDelete(t *testing.T) {
	directory := t.TempDir()
	options := hatDataStructure.SpillableArrangementOptions{
		Directory:        directory,
		MemoryLimitBytes: 1,
	}
	arrangement, err := hatDataStructure.NewSpillableArrangement(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Set("keep", []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Set("remove", []byte("no")); err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Flush(); err != nil {
		t.Fatal(err)
	}
	if !arrangement.Delete("remove") {
		t.Fatal("expected remove to delete an existing key")
	}
	if err := arrangement.Flush(); err != nil {
		t.Fatal(err)
	}
	spillPath := arrangement.SpillPath()
	if err := arrangement.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := hatDataStructure.OpenSpillableArrangement(spillPath, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, ok, err := reopened.Get("keep")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "yes" {
		t.Fatalf("keep = (%q, %t), want (yes, true)", value, ok)
	}
	if _, ok, err := reopened.Get("remove"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("flushed delete reappeared after reopen")
	}
}
