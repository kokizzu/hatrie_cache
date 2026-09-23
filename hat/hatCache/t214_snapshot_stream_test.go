package hatCache

import (
	"bytes"
	"io"
	"testing"
)

func TestT214SnapshotStreamRoundTripWithoutSharedFilesystem(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertCounter("counter", 42)
	source.UpsertString("string", "streamed")
	source.UpsertBytes("bytes", []byte("payload"))

	target := newTestTrie(t)
	reader, writer := io.Pipe()
	writeErr := make(chan error, 1)
	go func() {
		err := source.WriteSnapshotToWithJournalSequenceAndFormat(writer, 73, SnapshotFormatGzipBinary)
		_ = writer.CloseWithError(err)
		writeErr <- err
	}()

	metadata, err := target.LoadSnapshotFromWithMetadata(reader)
	if err != nil {
		t.Fatalf("LoadSnapshotFromWithMetadata() error = %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("WriteSnapshotToWithJournalSequenceAndFormat() error = %v", err)
	}
	if metadata.JournalSequence != 73 {
		t.Fatalf("journal sequence = %d, want 73", metadata.JournalSequence)
	}
	if got := target.GetCounter("counter"); got != 42 {
		t.Fatalf("counter = %d, want 42", got)
	}
	if got := target.GetString("string"); got != "streamed" {
		t.Fatalf("string = %q, want streamed", got)
	}
	if got := target.GetBytes("bytes"); !bytes.Equal(got, []byte("payload")) {
		t.Fatalf("bytes = %q, want payload", got)
	}
}

func TestT214SnapshotStreamRejectsTruncatedInputWithoutMutation(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("incoming", "value")
	var snapshot bytes.Buffer
	if err := source.WriteSnapshotToWithJournalSequenceAndFormat(&snapshot, 11, SnapshotFormatBinary); err != nil {
		t.Fatalf("WriteSnapshotToWithJournalSequenceAndFormat() error = %v", err)
	}
	data := snapshot.Bytes()
	if len(data) < 2 {
		t.Fatal("snapshot unexpectedly too small")
	}

	target := newTestTrie(t)
	target.UpsertString("keep", "before")
	if _, err := target.LoadSnapshotFromWithMetadata(bytes.NewReader(data[:len(data)-1])); err == nil {
		t.Fatal("LoadSnapshotFromWithMetadata() error = nil, want truncated stream error")
	}
	if got := target.GetString("keep"); got != "before" {
		t.Fatalf("keep = %q, want before after rejected stream", got)
	}
}
