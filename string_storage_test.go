package hatriecache

import (
	"bytes"
	"path/filepath"
	"testing"
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
