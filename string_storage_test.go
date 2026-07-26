package hatriecache

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func TestRawScalarTypeTransitionsAndCompaction(t *testing.T) {
	trie := newTestTrie(t)

	trie.UpsertString("value", "middle")
	if got := trie.GetString("value"); got != "middle" {
		t.Fatalf("GetString(value) = %q, want middle", got)
	}
	if got, err := trie.PrependStringChecked("value", "pre-"); err != nil || got != "pre-middle" {
		t.Fatalf("PrependStringChecked(value) = %q/%v, want pre-middle", got, err)
	}
	if got, err := trie.AppendStringChecked("value", "-post"); err != nil || got != "pre-middle-post" {
		t.Fatalf("AppendStringChecked(value) = %q/%v, want pre-middle-post", got, err)
	}

	wantBytes := []byte{0, 1, 2, 0xff}
	trie.UpsertBytes("value", wantBytes)
	wantBytes[0] = 9
	if got := trie.GetBytes("value"); !bytes.Equal(got, []byte{0, 1, 2, 0xff}) {
		t.Fatalf("GetBytes(value) = %v, want caller-independent bytes", got)
	}
	if got := trie.GetString("value"); got != "" {
		t.Fatalf("GetString(bytes value) = %q, want empty type mismatch", got)
	}

	trie.UpsertString("value", "")
	trie.UpsertBytes("empty-bytes", []byte{})
	trie.UpsertString("stable", "keep")
	if _, err := trie.CompactMemory(); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := trie.GetStringChecked("value"); err != nil || !ok || got != "" {
		t.Fatalf("GetStringChecked(empty string) = %q/%v/%v, want present empty string", got, ok, err)
	}
	if got, err := trie.GetBytesChecked("empty-bytes"); err != nil || len(got) != 0 || !trie.Exists("empty-bytes") {
		t.Fatalf("GetBytesChecked(empty bytes) = %v/%v, want present empty bytes", got, err)
	}
	if got := trie.GetString("stable"); got != "keep" {
		t.Fatalf("GetString(stable) = %q, want keep", got)
	}

	path := filepath.Join(t.TempDir(), "raw-scalars.snapshot")
	if err := trie.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
		t.Fatal(err)
	}
	restored := newTestTrie(t)
	if err := restored.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := restored.GetStringChecked("value"); err != nil || !ok || got != "" {
		t.Fatalf("restored empty string = %q/%v/%v, want present", got, ok, err)
	}
	if got, err := restored.GetBytesChecked("empty-bytes"); err != nil || len(got) != 0 || !restored.Exists("empty-bytes") {
		t.Fatalf("restored empty bytes = %v/%v, want present", got, err)
	}
}

func TestStringStoragePutRevivesDeletedIndex(t *testing.T) {
	storage := CreateStringStorage()
	index := storage.Add("first")
	liveTail := storage.Add("tail")
	storage.Del(index)
	if !storage.reusables.Has(index) {
		t.Fatalf("deleted index %d is not reusable", index)
	}

	storage.Put(index, "revived")
	if got := storage.Get(index); got != "revived" {
		t.Fatalf("Get(revived index) = %q, want revived", got)
	}
	if storage.reusables.Has(index) || storage.reusables.Len() != 0 {
		t.Fatalf("reusable state after Put = has %v len %d, want false/0", storage.reusables.Has(index), storage.reusables.Len())
	}
	if next := storage.Add("next"); next == index {
		t.Fatalf("Add() reused revived index %d", index)
	} else if next <= liveTail {
		t.Fatalf("Add() index = %d, want append after live tail %d", next, liveTail)
	}
}

func TestStringStorageReplaceActiveLeavesReusableIndexesUnchanged(t *testing.T) {
	storage := CreateStringStorage()
	reusable := storage.Add("reusable")
	active := storage.Add("active")
	storage.Del(reusable)
	if !storage.reusables.Has(reusable) || storage.reusables.Len() != 1 {
		t.Fatalf("initial reusable state = has %v len %d, want true/1", storage.reusables.Has(reusable), storage.reusables.Len())
	}

	storage.replaceActive(active, "updated")
	storage.replaceActive(-1, "invalid")
	storage.replaceActive(int32(len(storage.array)), "invalid")
	if got := storage.Get(active); got != "updated" {
		t.Fatalf("Get(active) = %q, want updated", got)
	}
	if !storage.reusables.Has(reusable) || storage.reusables.Len() != 1 {
		t.Fatalf("reusable state after replaceActive = has %v len %d, want true/1", storage.reusables.Has(reusable), storage.reusables.Len())
	}
	if next := storage.Add("next"); next != reusable {
		t.Fatalf("Add() index = %d, want reusable %d", next, reusable)
	}
}

func TestLiveStringReplacementPreservesCacheBehavior(t *testing.T) {
	trie := newTestTrie(t)
	now := time.Unix(350, 0)
	trie.now = func() time.Time { return now }

	trie.UpsertString("key", "value")
	index := trie.Get("key").Index
	if !trie.Expire("key", time.Minute) {
		t.Fatal("Expire(key) = false, want true")
	}
	before := trie.Stats()
	trie.UpsertString("key", "value")
	if got := trie.GetString("key"); got != "value" {
		t.Fatalf("GetString(key) = %q, want value", got)
	}
	if got := trie.Get("key"); got.Index != index || !got.IsStringAtRaws() {
		t.Fatalf("duplicate string location = %+v, want string index %d", got, index)
	}
	if got := trie.TTL("key"); got != NoTTL {
		t.Fatalf("TTL after duplicate UpsertString() = %s, want NoTTL", got)
	}
	if writes := trie.Stats().Writes; writes != before.Writes+1 {
		t.Fatalf("writes after duplicate UpsertString() = %d, want %d", writes, before.Writes+1)
	}

	if got, err := trie.AppendStringChecked("key", "-tail"); err != nil || got != "value-tail" {
		t.Fatalf("AppendStringChecked() = %q/%v, want value-tail/nil", got, err)
	}
	if got, err := trie.PrependStringChecked("key", "head-"); err != nil || got != "head-value-tail" {
		t.Fatalf("PrependStringChecked() = %q/%v, want head-value-tail/nil", got, err)
	}
	if got := trie.Get("key"); got.Index != index {
		t.Fatalf("string location after append/prepend = %d, want %d", got.Index, index)
	}
}
