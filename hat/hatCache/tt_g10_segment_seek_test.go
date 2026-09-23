package hatCache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestTTG10ReplaySegmentBoundsSelectsOverlappingSegments(t *testing.T) {
	segments := []commandJournalSegment{
		{path: "segment-1", start: 1, end: 100},
		{path: "segment-2", start: 101, end: 200},
		{path: "segment-3", start: 201, end: 300},
		{path: "segment-4", start: 301, end: 400},
	}

	tests := []struct {
		name                string
		after, target       uint64
		wantFirst, wantLast int
	}{
		{name: "full replay", wantFirst: 0, wantLast: 4},
		{name: "after middle", after: 250, wantFirst: 2, wantLast: 4},
		{name: "bounded middle", after: 150, target: 250, wantFirst: 1, wantLast: 3},
		{name: "after end", after: 400, wantFirst: 4, wantLast: 4},
		{name: "target before after", after: 250, target: 200, wantFirst: 2, wantLast: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first, last := commandJournalReplaySegmentBounds(segments, test.after, test.target)
			if first != test.wantFirst || last != test.wantLast {
				t.Fatalf("commandJournalReplaySegmentBounds(%d, %d) = %d, %d; want %d, %d", test.after, test.target, first, last, test.wantFirst, test.wantLast)
			}
		})
	}
}

func TestTTG10RangedReplayMatchesFullReplayAndSkipsOldSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	options := CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 64,
		SegmentMaxBytes:     256,
		RetainedSegments:    1024,
	}
	journal, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatal(err)
	}
	source := CreateHatTrie()
	const totalEntries = 256
	for index := 0; index < totalEntries; index++ {
		response := journal.ExecuteCommand(source, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("tt-g10-replay-key-%03d", index),
			Value:   "tt-g10-replay-value",
		})
		if !response.OK {
			source.Destroy()
			journal.Close()
			t.Fatalf("journal.ExecuteCommand(%d) = %#v", index, response)
		}
	}
	source.Destroy()

	afterSequence := journal.compactedThrough
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	segments, err := listCommandJournalSegments(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) < 2 {
		t.Fatalf("segmented journal produced %d archived segments, want at least 2", len(segments))
	}
	if afterSequence == 0 {
		afterSequence = segments[0].end
	}
	reference := CreateHatTrie()
	if _, err := scanCommandJournalSetWithEncryption(path, true, options.Encryption, func(entry commandJournalEntry) error {
		if entry.Checkpoint || entry.Sequence <= afterSequence {
			return nil
		}
		return executeCommandForReplay(reference, entry.Request)
	}); err != nil {
		reference.Destroy()
		t.Fatalf("reference scan error = %v", err)
	}
	journal, err = OpenCommandJournalWithOptions(path, options)
	if err != nil {
		reference.Destroy()
		t.Fatal(err)
	}
	partial := CreateHatTrie()
	if _, err := journal.Replay(partial, afterSequence); err != nil {
		reference.Destroy()
		partial.Destroy()
		journal.Close()
		t.Fatalf("ranged Replay() error = %v", err)
	}
	for index := 0; index < totalEntries; index++ {
		sequence := uint64(index + 1)
		key := fmt.Sprintf("tt-g10-replay-key-%03d", index)
		if got, want := partial.GetString(key), reference.GetString(key); sequence > afterSequence && got != want {
			reference.Destroy()
			partial.Destroy()
			journal.Close()
			t.Fatalf("partial replay key %q = %q, want reference value %q", key, got, want)
		}
		if sequence <= afterSequence && partial.Exists(key) {
			reference.Destroy()
			partial.Destroy()
			journal.Close()
			t.Fatalf("partial replay unexpectedly applied key %q", key)
		}
	}
	reference.Destroy()
	partial.Destroy()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	oldSegment := segments[0]
	data, err := os.ReadFile(oldSegment.path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatalf("archived segment %q is empty", oldSegment.path)
	}
	data[0] ^= 0xff
	if err := os.WriteFile(oldSegment.path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := scanCommandJournalSetAfterSequenceWithEncryption(path, true, oldSegment.end, 0, options.Encryption, nil); err != nil {
		t.Fatalf("ranged scan after archived segment failed: %v", err)
	}
}

func BenchmarkTTG10ReplayExistingFullSegmentScan(b *testing.B) {
	path := filepath.Join(b.TempDir(), "commands.journal")
	options := CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 64,
		SegmentMaxBytes:     512,
		RetainedSegments:    1024,
	}
	journal, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		b.Fatal(err)
	}
	source := CreateHatTrie()
	const totalEntries = 4096
	for index := 0; index < totalEntries; index++ {
		response := journal.ExecuteCommand(source, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("tt-g10-key-%04d", index),
			Value:   "tt-g10-value",
		})
		if !response.OK {
			source.Destroy()
			journal.Close()
			b.Fatalf("journal.ExecuteCommand(%d) = %#v", index, response)
		}
	}
	source.Destroy()
	if err := journal.Close(); err != nil {
		b.Fatal(err)
	}
	journal, err = OpenCommandJournalWithOptions(path, options)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = journal.Close() })

	const afterSequence = totalEntries - 128
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		replayed := CreateHatTrie()
		if _, err := journal.Replay(replayed, afterSequence); err != nil {
			replayed.Destroy()
			b.Fatal(err)
		}
		replayed.Destroy()
	}
}
