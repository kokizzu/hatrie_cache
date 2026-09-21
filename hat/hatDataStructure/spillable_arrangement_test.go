package hatDataStructure_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestSpillableArrangementSpillsReadsDeletesAndCompacts(t *testing.T) {
	directory := t.TempDir()
	arrangement, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{
		Directory:        directory,
		MemoryLimitBytes: 8,
		MaxDiskBytes:     1 << 20,
	})
	if err != nil {
		t.Fatalf("create arrangement: %v", err)
	}
	defer arrangement.Close()

	values := map[string][]byte{
		"a": []byte("alpha"),
		"b": []byte("bravo"),
		"c": []byte("charlie"),
	}
	for key, value := range values {
		if err := arrangement.Set(key, value); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
	}
	values["a"][0] = 'X'
	stats := arrangement.Stats()
	if stats.Entries != len(values) || stats.ColdEntries == 0 || stats.HotBytes > 8 {
		t.Fatalf("stats after spill = %+v, want entries/cold/hot bound", stats)
	}

	for key, want := range map[string][]byte{"a": []byte("alpha"), "b": []byte("bravo"), "c": []byte("charlie")} {
		got, found, err := arrangement.Get(key)
		if err != nil || !found || !reflect.DeepEqual(got, want) {
			t.Fatalf("Get(%q) = %#v/%t/%v, want %#v/true/nil", key, got, found, err, want)
		}
		got[0] = 'X'
		again, found, err := arrangement.Get(key)
		if err != nil || !found || !reflect.DeepEqual(again, want) {
			t.Fatalf("Get(%q) after caller mutation = %#v/%t/%v, want %#v/true/nil", key, again, found, err, want)
		}
	}

	rows, err := arrangement.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if got := spillableArrangementKeys(rows); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("Snapshot keys = %#v, want sorted keys", got)
	}
	if !arrangement.Delete("b") || arrangement.Delete("missing") {
		t.Fatal("Delete() result mismatch")
	}
	if _, found, err := arrangement.Get("b"); err != nil || found {
		t.Fatalf("Get(deleted) = found=%t err=%v, want false/nil", found, err)
	}
	beforeCompact := arrangement.Stats()
	if err := arrangement.Compact(); err != nil {
		t.Fatalf("Compact(): %v", err)
	}
	afterCompact := arrangement.Stats()
	if afterCompact.DiskBytes > beforeCompact.DiskBytes || afterCompact.Entries != 2 {
		t.Fatalf("stats after compaction = %+v, before=%+v", afterCompact, beforeCompact)
	}
	rows, err = arrangement.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() after compaction: %v", err)
	}
	if got := spillableArrangementKeys(rows); !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Fatalf("Snapshot keys after compaction = %#v, want a/c", got)
	}
}

func TestSpillableArrangementValidatesLimitsCorruptionAndClose(t *testing.T) {
	if _, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{MemoryLimitBytes: -1}); !errors.Is(err, hatDataStructure.ErrSpillableArrangementLimitInvalid) {
		t.Fatalf("negative memory limit error = %v, want limit error", err)
	}
	arrangement, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{
		Directory:        t.TempDir(),
		MemoryLimitBytes: 1,
		MaxDiskBytes:     32,
	})
	if err != nil {
		t.Fatalf("create limited arrangement: %v", err)
	}
	if err := arrangement.Set("", []byte("value")); !errors.Is(err, hatDataStructure.ErrSpillableArrangementKeyRequired) {
		t.Fatalf("empty key error = %v, want key error", err)
	}
	if err := arrangement.Set("key", []byte("a very long value")); !errors.Is(err, hatDataStructure.ErrSpillableArrangementDiskLimit) {
		t.Fatalf("disk limit error = %v, want disk limit", err)
	}
	if arrangement.Stats().Entries != 0 {
		t.Fatalf("failed Set changed entries: %+v", arrangement.Stats())
	}
	if err := arrangement.Set("key", []byte("value")); err != nil {
		t.Fatalf("Set(valid): %v", err)
	}
	if err := arrangement.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := arrangement.Set("other", []byte("value")); !errors.Is(err, hatDataStructure.ErrSpillableArrangementClosed) {
		t.Fatalf("Set(after close) error = %v, want closed", err)
	}
	if _, _, err := arrangement.Get("key"); !errors.Is(err, hatDataStructure.ErrSpillableArrangementClosed) {
		t.Fatalf("Get(after close) error = %v, want closed", err)
	}

	corruptDir := t.TempDir()
	corrupt, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{
		Directory:        corruptDir,
		MemoryLimitBytes: 1,
		MaxDiskBytes:     1 << 20,
	})
	if err != nil {
		t.Fatalf("create corruption arrangement: %v", err)
	}
	if err := corrupt.Set("key", []byte("value")); err != nil {
		t.Fatalf("Set(corruption): %v", err)
	}
	if err := corrupt.Flush(); err != nil {
		t.Fatalf("Flush(corruption): %v", err)
	}
	path := corrupt.SpillPath()
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open spill file: %v", err)
	}
	if _, err := file.WriteAt([]byte("bad!"), 0); err != nil {
		t.Fatalf("corrupt spill file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close spill file: %v", err)
	}
	if _, found, err := corrupt.Get("key"); !errors.Is(err, hatDataStructure.ErrSpillableArrangementCorrupt) || found {
		t.Fatalf("Get(corrupt) = found=%t err=%v, want false/corrupt", found, err)
	}
	if err := corrupt.Close(); err != nil {
		t.Fatalf("Close(corruption): %v", err)
	}

	flushLimited, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{
		Directory:        t.TempDir(),
		MemoryLimitBytes: 1 << 20,
		MaxDiskBytes:     1,
	})
	if err != nil {
		t.Fatalf("create flush-limited arrangement: %v", err)
	}
	if err := flushLimited.Set("key", []byte("value")); err != nil {
		t.Fatalf("Set(flush-limited): %v", err)
	}
	if err := flushLimited.Flush(); !errors.Is(err, hatDataStructure.ErrSpillableArrangementDiskLimit) {
		t.Fatalf("Flush(disk-limited) error = %v, want disk limit", err)
	}
	if stats := flushLimited.Stats(); stats.ColdEntries != 0 || stats.DiskBytes != 0 || stats.HotBytes != 5 {
		t.Fatalf("failed Flush changed state: %+v", stats)
	}
	if err := flushLimited.Close(); err != nil {
		t.Fatalf("Close(flush-limited): %v", err)
	}

	deletedOnly, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{
		Directory:        t.TempDir(),
		MemoryLimitBytes: 1,
		MaxDiskBytes:     1 << 20,
	})
	if err != nil {
		t.Fatalf("create deleted-only arrangement: %v", err)
	}
	if err := deletedOnly.Set("key", []byte("value")); err != nil {
		t.Fatalf("Set(deleted-only): %v", err)
	}
	if !deletedOnly.Delete("key") {
		t.Fatal("Delete(deleted-only) = false")
	}
	if err := deletedOnly.Compact(); err != nil {
		t.Fatalf("Compact(deleted-only): %v", err)
	}
	if stats := deletedOnly.Stats(); stats.Entries != 0 || stats.DiskBytes != 0 {
		t.Fatalf("deleted-only compaction stats = %+v, want empty segment", stats)
	}
	if err := deletedOnly.Close(); err != nil {
		t.Fatalf("Close(deleted-only): %v", err)
	}

	owned, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{})
	if err != nil {
		t.Fatalf("create owned arrangement: %v", err)
	}
	ownedDirectory := filepath.Dir(owned.SpillPath())
	if err := owned.Close(); err != nil {
		t.Fatalf("Close(owned): %v", err)
	}
	if _, err := os.Stat(ownedDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned spill directory stat error = %v, want not exist", err)
	}
}

func spillableArrangementKeys(rows []hatDataStructure.SpillableArrangementEntry) []string {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, row.Key)
	}
	sort.Strings(keys)
	return keys
}
