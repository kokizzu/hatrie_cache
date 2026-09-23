package hatCache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestT213ScheduledSnapshotPublishesAndVerifiesCheckpointManifest(t *testing.T) {
	trie := newTestTrie(t)

	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "scheduled", Value: "snapshot"}); !response.OK {
		t.Fatalf("ExecuteCommand() error = %s", response.Message)
	}

	snapshotPath := filepath.Join(t.TempDir(), "snapshot.hc")
	manifestPath := snapshotPath + ".manifest.json"
	scheduler, err := StartScheduledSnapshots(context.Background(), journal, trie, ScheduledSnapshotOptions{
		Interval:       time.Hour,
		SnapshotPath:   snapshotPath,
		ManifestPath:   manifestPath,
		Format:         SnapshotFormatBinary,
		RunImmediately: false,
	})
	if err != nil {
		t.Fatalf("StartScheduledSnapshots() error = %v", err)
	}
	t.Cleanup(func() { _ = scheduler.Close() })

	result, err := scheduler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Manifest.Snapshot.JournalSequence != journal.Sequence() {
		t.Fatalf("snapshot sequence = %d, want journal sequence %d", result.Manifest.Snapshot.JournalSequence, journal.Sequence())
	}
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("snapshot was not published: %v", err)
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("manifest was not published: %v", err)
	}
	if _, err := VerifyScheduledSnapshotManifest(manifestPath); err != nil {
		t.Fatalf("VerifyScheduledSnapshotManifest() error = %v", err)
	}

	loaded := newTestTrie(t)
	if err := loaded.LoadSnapshot(snapshotPath); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got := loaded.GetString("scheduled"); got != "snapshot" {
		t.Fatalf("loaded scheduled value = %q, want snapshot", got)
	}
}

func TestT213ScheduledSnapshotRunsPeriodicallyAndStops(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	t.Cleanup(func() { _ = journal.Close() })

	scheduler, err := StartScheduledSnapshots(context.Background(), journal, trie, ScheduledSnapshotOptions{
		Interval:       10 * time.Millisecond,
		SnapshotPath:   filepath.Join(t.TempDir(), "periodic.hc"),
		RunImmediately: true,
	})
	if err != nil {
		t.Fatalf("StartScheduledSnapshots() error = %v", err)
	}

	deadline := time.NewTimer(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		status := scheduler.Status()
		if status.Completed >= 2 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("scheduler completed %d snapshots, want at least 2", status.Completed)
		case <-ticker.C:
		}
	}
	if err := scheduler.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	completed := scheduler.Status().Completed
	time.Sleep(30 * time.Millisecond)
	if got := scheduler.Status().Completed; got != completed {
		t.Fatalf("completed snapshots after Close() = %d, want %d", got, completed)
	}
}

func TestT213ScheduledSnapshotValidatesOptionsAndDetectsCorruption(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	t.Cleanup(func() { _ = journal.Close() })

	invalid := []ScheduledSnapshotOptions{
		{SnapshotPath: filepath.Join(t.TempDir(), "snapshot.hc")},
		{Interval: time.Second},
	}
	for _, options := range invalid {
		if _, err := StartScheduledSnapshots(context.Background(), journal, trie, options); err == nil {
			t.Fatalf("StartScheduledSnapshots(%#v) error = nil", options)
		}
	}

	snapshotPath := filepath.Join(t.TempDir(), "snapshot.hc")
	manifestPath := snapshotPath + ".manifest.json"
	scheduler, err := StartScheduledSnapshots(context.Background(), journal, trie, ScheduledSnapshotOptions{
		Interval:     time.Hour,
		SnapshotPath: snapshotPath,
		ManifestPath: manifestPath,
		Format:       SnapshotFormatBinary,
	})
	if err != nil {
		t.Fatalf("StartScheduledSnapshots() error = %v", err)
	}
	if _, err := scheduler.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if err := os.WriteFile(snapshotPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatalf("os.WriteFile(corrupt) error = %v", err)
	}
	if _, err := VerifyScheduledSnapshotManifest(manifestPath); !errors.Is(err, ErrSnapshotManifestMismatch) {
		t.Fatalf("VerifyScheduledSnapshotManifest(corrupt) error = %v, want manifest mismatch", err)
	}
	if err := scheduler.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
