package hatJournal_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hatrie_cache/hat/hatJournal"
)

func TestInspectReportsJournalMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	data := []byte("{\"version\":1,\"sequence\":1,\"request\":{\"command\":\"SET\"}}\n" +
		"{\"version\":1,\"sequence\":2,\"checkpoint\":true}\n" +
		"{\"version\":1,\"sequence\":3,\"request\":{\"command\":\"DEL\"}}\npartial")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	inspection, err := hatJournal.Inspect(path, hatJournal.InspectOptions{})
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if inspection.RecordCount != 3 || inspection.FirstSequence != 1 || inspection.LastSequence != 3 {
		t.Fatalf("inspection record metadata = %#v, want records 1 through 3", inspection)
	}
	if inspection.CompactedThrough != 2 || !inspection.TruncatedTail {
		t.Fatalf("inspection checkpoint/tail = %#v, want checkpoint 2 and truncated tail", inspection)
	}
	if inspection.Active.Format != hatJournal.FormatJSON || inspection.Active.ValidBytes >= inspection.Active.Size {
		t.Fatalf("inspection active file = %#v, want JSON valid prefix", inspection.Active)
	}
}

func TestInspectValidatesSegmentedSequenceContinuity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	segmentDir := path + ".segments"
	if err := os.MkdirAll(segmentDir, 0o700); err != nil {
		t.Fatal(err)
	}
	segment := filepath.Join(segmentDir, "00000000000000000001-00000000000000000001.journal")
	if err := os.WriteFile(segment, []byte("{\"version\":1,\"sequence\":1,\"request\":{\"command\":\"SET\"}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{\"version\":1,\"sequence\":1,\"checkpoint\":true}\n{\"version\":1,\"sequence\":2,\"request\":{\"command\":\"SET\"}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	inspection, err := hatJournal.Inspect(path, hatJournal.InspectOptions{Segmented: true})
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if len(inspection.Segments) != 1 || inspection.Active.FirstSequence != 1 || inspection.LastSequence != 2 {
		t.Fatalf("segmented inspection = %#v", inspection)
	}

	if err := os.WriteFile(path, []byte("{\"version\":1,\"sequence\":4,\"request\":{\"command\":\"SET\"}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hatJournal.Inspect(path, hatJournal.InspectOptions{Segmented: true}); err == nil {
		t.Fatal("Inspect() error = nil for a segment gap")
	}
}

func TestInspectReadsBinaryJournalMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	payload := []byte{
		3, 1, 0, // binary version, sequence, checkpoint
		3, 'S', 'E', 'T', // command
		0, 0, 0, // key, value, subkey
		0, 0, 0, // absent priority, TTL, and expiry
		0, 0, 0, // values, pairs, and outbox
	}
	record := append([]byte{'h', 'c', 'j', 'n', 1}, byte(len(payload)))
	record = append(record, payload...)
	if err := os.WriteFile(path, record, 0o600); err != nil {
		t.Fatal(err)
	}

	inspection, err := hatJournal.Inspect(path, hatJournal.InspectOptions{})
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if inspection.Active.Format != hatJournal.FormatBinary || inspection.LastSequence != 1 || inspection.RecordCount != 1 {
		t.Fatalf("binary inspection = %#v", inspection)
	}

	tooLarge := append([]byte{'h', 'c', 'j', 'n', 1}, make([]byte, binary.MaxVarintLen64)...)
	if err := os.WriteFile(path, tooLarge, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hatJournal.Inspect(path, hatJournal.InspectOptions{}); err == nil {
		t.Fatal("Inspect() error = nil for an invalid binary record size")
	}
}

func TestValidateOptionsNormalizesFormatAndRejectsInvalidValues(t *testing.T) {
	options, err := hatJournal.ValidateOptions(hatJournal.Options{
		Format:              " bin ",
		GroupCommitWindow:   time.Millisecond,
		GroupCommitMaxBatch: 2,
		SegmentMaxBytes:     1024,
		RetainedSegments:    2,
	})
	if err != nil {
		t.Fatalf("ValidateOptions() error = %v", err)
	}
	if options.Format != hatJournal.FormatBinary {
		t.Fatalf("normalized format = %q, want binary", options.Format)
	}
	if _, err := hatJournal.ValidateOptions(hatJournal.Options{GroupCommitMaxBatch: 0}); err == nil {
		t.Fatal("ValidateOptions() error = nil for zero batch")
	}
}
