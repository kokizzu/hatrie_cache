//go:build !mz003baseline

package hatCache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMZ003LoadSnapshotWithProgressReportsMonotonicProgress(t *testing.T) {
	source := newTestTrie(t)
	target := newTestTrie(t)
	defer source.Destroy()
	defer target.Destroy()

	const entries = 64
	for index := 0; index < entries; index++ {
		source.UpsertString(fmt.Sprintf("mz003:%03d", index), fmt.Sprintf("value-%03d", index))
	}
	path := filepath.Join(t.TempDir(), "snapshot.hc")
	if err := source.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
		t.Fatalf("SaveSnapshotWithFormat() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(snapshot) error = %v", err)
	}

	var progress []SnapshotRestoreProgress
	metadata, err := target.LoadSnapshotWithProgress(path, func(update SnapshotRestoreProgress) error {
		progress = append(progress, update)
		return nil
	})
	if err != nil {
		t.Fatalf("LoadSnapshotWithProgress() error = %v", err)
	}
	if metadata.JournalSequence != 0 {
		t.Fatalf("JournalSequence = %d, want 0", metadata.JournalSequence)
	}
	if len(progress) != entries {
		t.Fatalf("progress callbacks = %d, want %d", len(progress), entries)
	}
	for index, update := range progress {
		if update.EntriesRead != uint64(index+1) {
			t.Fatalf("progress[%d].EntriesRead = %d, want %d", index, update.EntriesRead, index+1)
		}
		if update.BytesRead <= 0 || update.BytesRead > info.Size() {
			t.Fatalf("progress[%d].BytesRead = %d, want (0, %d]", index, update.BytesRead, info.Size())
		}
		if update.TotalBytes != info.Size() {
			t.Fatalf("progress[%d].TotalBytes = %d, want %d", index, update.TotalBytes, info.Size())
		}
		if index > 0 && update.BytesRead < progress[index-1].BytesRead {
			t.Fatalf("progress[%d].BytesRead = %d moved backwards from %d", index, update.BytesRead, progress[index-1].BytesRead)
		}
	}
	if got := target.GetString("mz003:063"); got != "value-063" {
		t.Fatalf("restored final value = %q, want value-063", got)
	}
}

func TestMZ003LoadSnapshotWithProgressAbortsBeforeCutover(t *testing.T) {
	source := newTestTrie(t)
	target := newTestTrie(t)
	defer source.Destroy()
	defer target.Destroy()

	source.UpsertString("mz003:new", "snapshot-value")
	target.UpsertString("mz003:live", "live-value")
	path := filepath.Join(t.TempDir(), "snapshot.hc")
	if err := source.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
		t.Fatalf("SaveSnapshotWithFormat() error = %v", err)
	}
	cancelErr := errors.New("stop hydration")
	_, err := target.LoadSnapshotWithProgress(path, func(update SnapshotRestoreProgress) error {
		if update.EntriesRead == 1 {
			return cancelErr
		}
		return nil
	})
	if !errors.Is(err, cancelErr) {
		t.Fatalf("LoadSnapshotWithProgress() error = %v, want %v", err, cancelErr)
	}
	if got := target.GetString("mz003:live"); got != "live-value" {
		t.Fatalf("live value after cancelled restore = %q, want live-value", got)
	}
	if got := target.GetString("mz003:new"); got != "" {
		t.Fatalf("cancelled restore installed snapshot value %q", got)
	}
}

func TestMZ003LoadSnapshotWithProgressSupportsSnapshotFormats(t *testing.T) {
	source := newTestTrie(t)
	defer source.Destroy()
	source.UpsertString("mz003:format", "format-value")

	formats := []SnapshotFormat{
		SnapshotFormatBinary,
		SnapshotFormatJSON,
		SnapshotFormatGzipJSON,
		SnapshotFormatGzipBestJSON,
	}
	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			target := newTestTrie(t)
			defer target.Destroy()
			path := filepath.Join(t.TempDir(), "snapshot.hc")
			if err := source.SaveSnapshotWithFormat(path, format); err != nil {
				t.Fatalf("SaveSnapshotWithFormat(%s) error = %v", format, err)
			}
			updates := 0
			if _, err := target.LoadSnapshotWithProgress(path, func(update SnapshotRestoreProgress) error {
				updates++
				return nil
			}); err != nil {
				t.Fatalf("LoadSnapshotWithProgress(%s) error = %v", format, err)
			}
			if updates != 1 {
				t.Fatalf("progress callbacks = %d, want 1", updates)
			}
			if got := target.GetString("mz003:format"); got != "format-value" {
				t.Fatalf("restored value = %q, want format-value", got)
			}
		})
	}
}

func TestMZ003LoadSnapshotWithProgressNilReceiver(t *testing.T) {
	var trie *HatTrie
	_, err := trie.LoadSnapshotWithProgress("unused", func(SnapshotRestoreProgress) error { return nil })
	if !errors.Is(err, ErrNilHatTrie) {
		t.Fatalf("LoadSnapshotWithProgress(nil receiver) error = %v, want ErrNilHatTrie", err)
	}
}
