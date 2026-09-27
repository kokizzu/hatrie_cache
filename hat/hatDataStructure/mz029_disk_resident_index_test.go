package hatDataStructure_test

import (
	"fmt"
	"os"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestMZ029DiskResidentIndexUsesSidecarUntilMutation(t *testing.T) {
	directory := t.TempDir()
	options := hatDataStructure.SpillableArrangementOptions{
		Directory:        directory,
		MemoryLimitBytes: 1,
	}
	arrangement, err := hatDataStructure.NewSpillableArrangement(options)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 4096; index++ {
		key := fmt.Sprintf("key-%04d", index)
		if err := arrangement.Set(key, []byte(key+"-value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := arrangement.Flush(); err != nil {
		t.Fatal(err)
	}
	spillPath := arrangement.SpillPath()
	if err := arrangement.Close(); err != nil {
		t.Fatal(err)
	}

	diskOptions := options
	diskOptions.DiskResidentIndex = true
	reopened, err := hatDataStructure.OpenSpillableArrangement(spillPath, diskOptions)
	if err != nil {
		t.Fatal(err)
	}
	if stats := reopened.Stats(); !stats.DiskResidentIndex || stats.IndexResidentEntries != 0 || stats.Entries != 4096 {
		t.Fatalf("disk-resident stats = %#v, want disk mode with zero resident entries and 4096 total", stats)
	}
	value, found, err := reopened.Get("key-3077")
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(value) != "key-3077-value" {
		t.Fatalf("disk-resident Get() = (%q, %t), want key-3077-value/true", value, found)
	}

	if err := reopened.Set("key-new", []byte("new-value")); err != nil {
		t.Fatal(err)
	}
	if !reopened.Delete("key-3077") {
		t.Fatal("disk-resident delete did not find key-3077")
	}
	if stats := reopened.Stats(); stats.DiskResidentIndex || stats.IndexResidentEntries != 4096 || stats.Entries != 4096 {
		t.Fatalf("post-mutation stats = %#v, want materialized 4096-entry index", stats)
	}
	if err := reopened.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	verified, err := hatDataStructure.OpenSpillableArrangement(spillPath, options)
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Close()
	if _, found, err := verified.Get("key-3077"); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("deleted key reappeared after materialized mutation")
	}
	if value, found, err := verified.Get("key-new"); err != nil {
		t.Fatal(err)
	} else if !found || string(value) != "new-value" {
		t.Fatalf("new key = (%q, %t), want new-value/true", value, found)
	}
}

func TestMZ029DiskResidentIndexFallsBackOnCorruption(t *testing.T) {
	directory := t.TempDir()
	options := hatDataStructure.SpillableArrangementOptions{
		Directory:        directory,
		MemoryLimitBytes: 1,
	}
	arrangement, err := hatDataStructure.NewSpillableArrangement(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Set("key", []byte("value")); err != nil {
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

	diskOptions := options
	diskOptions.DiskResidentIndex = true
	reopened, err := hatDataStructure.OpenSpillableArrangement(spillPath, diskOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if stats := reopened.Stats(); stats.DiskResidentIndex || stats.IndexResidentEntries != 1 {
		t.Fatalf("corrupt-sidecar stats = %#v, want safe map fallback", stats)
	}
	if value, found, err := reopened.Get("key"); err != nil || !found || string(value) != "value" {
		t.Fatalf("fallback Get() = (%q, %t, %v), want value/true/nil", value, found, err)
	}
}
