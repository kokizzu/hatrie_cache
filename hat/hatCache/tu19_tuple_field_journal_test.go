package hatCache

import (
	"encoding/base64"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTU19TupleFieldCommandsJournalAndReplay(t *testing.T) {
	format, err := hatDataStructure.NewTupleFormat(17, []hatDataStructure.TupleFieldSpec{
		{Name: "count", Type: hatDataStructure.TupleFieldInt64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
		{Name: "payload", Type: hatDataStructure.TupleFieldBytes},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	original, err := format.PackVersioned([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleInt64(10),
		hatDataStructure.TupleString("before"),
		hatDataStructure.TupleBytes([]byte("abcd")),
	})
	if err != nil {
		t.Fatalf("PackVersioned() error = %v", err)
	}
	originalBytes, err := hatDataStructure.MarshalVersionedTuple(original)
	if err != nil {
		t.Fatalf("MarshalVersionedTuple() error = %v", err)
	}
	journalPath := t.TempDir() + "/commands.log"
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	trie := newTestTrie(t)

	stored := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command:     "TUPLESET",
		Key:         "order:1",
		BinaryValue: originalBytes,
	})
	if !stored.OK {
		t.Fatalf("TUPLESET response = %#v", stored)
	}

	updated := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "TUPLEUPDATE",
		Key:     "order:1",
		Values: []any{
			map[string]any{"index": 0, "kind": "add_int64", "delta": 5},
			map[string]any{"index": 1, "kind": "set", "value": base64.StdEncoding.EncodeToString([]byte("after"))},
			map[string]any{"index": 2, "kind": "splice", "start": 1, "remove": 2, "insert": base64.StdEncoding.EncodeToString([]byte("XY"))},
		},
	})
	if !updated.OK {
		t.Fatalf("TUPLEUPDATE response = %#v", updated)
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "TUPLEGET", Key: "order:1"})
	if !read.OK {
		t.Fatalf("TUPLEGET response = %#v", read)
	}

	want, err := format.PackVersioned([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleInt64(15),
		hatDataStructure.TupleString("after"),
		hatDataStructure.TupleBytes([]byte("aXYd")),
	})
	if err != nil {
		t.Fatalf("PackVersioned(want) error = %v", err)
	}
	wantBytes, err := hatDataStructure.MarshalVersionedTuple(want)
	if err != nil {
		t.Fatalf("MarshalVersionedTuple(want) error = %v", err)
	}
	if read.Value != base64.StdEncoding.EncodeToString(wantBytes) {
		t.Fatalf("TUPLEGET value = %q, want %q", read.Value, base64.StdEncoding.EncodeToString(wantBytes))
	}

	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	reopened, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("reopen journal error = %v", err)
	}
	defer reopened.Close()
	restored := newTestTrie(t)
	if _, err := reopened.Replay(restored, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	replayed := restored.ExecuteCommand(CacheCommandRequest{Command: "TUPLEGET", Key: "order:1"})
	if replayed.Value != read.Value {
		t.Fatalf("replayed TUPLEGET value = %q, want %q", replayed.Value, read.Value)
	}
}

func TestTU19TupleCommandsRejectInvalidInputWithoutMutation(t *testing.T) {
	format, err := hatDataStructure.NewTupleFormat(23, []hatDataStructure.TupleFieldSpec{{Name: "count", Type: hatDataStructure.TupleFieldInt64}})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	original, err := format.PackVersioned([]hatDataStructure.TupleFieldValue{hatDataStructure.TupleInt64(3)})
	if err != nil {
		t.Fatalf("PackVersioned() error = %v", err)
	}
	originalBytes, err := hatDataStructure.MarshalVersionedTuple(original)
	if err != nil {
		t.Fatalf("MarshalVersionedTuple() error = %v", err)
	}
	journal, err := OpenCommandJournal(t.TempDir() + "/commands.log")
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	trie := newTestTrie(t)
	stored := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "TUPLESET",
		Key:     "counter:1",
		Value:   base64.StdEncoding.EncodeToString(originalBytes),
	})
	if !stored.OK {
		t.Fatalf("TUPLESET response = %#v", stored)
	}
	invalid := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "TUPLEUPDATE",
		Key:     "counter:1",
		Values: []any{
			map[string]any{"index": 0, "kind": "add_int64", "delta": 1},
			map[string]any{"index": 0, "kind": "add_int64", "delta": 1},
		},
	})
	if invalid.OK {
		t.Fatalf("duplicate update unexpectedly succeeded: %#v", invalid)
	}
	read := trie.ExecuteCommand(CacheCommandRequest{Command: "TUPLEGET", Key: "counter:1"})
	if !read.OK || read.Value != base64.StdEncoding.EncodeToString(originalBytes) {
		t.Fatalf("tuple after rejected update = %#v, want original payload", read)
	}
	badSet := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "TUPLESET", Key: "bad", Value: "not-base64"})
	if badSet.OK {
		t.Fatalf("invalid TUPLESET unexpectedly succeeded: %#v", badSet)
	}
}
