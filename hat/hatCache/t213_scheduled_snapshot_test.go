//go:build t213

package hatCache

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestT213ScheduledSnapshotPublishesCheckpointManifest(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "scheduled", Value: "before"}); !response.OK {
		t.Fatalf("initial SET response = %#v", response)
	}

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	destination := filepath.Join(t.TempDir(), "latest.snapshot")
	scheduler, err := NewScheduledSnapshotter(journal, trie, ScheduledSnapshotOptions{
		Interval:    time.Hour,
		Destination: destination,
		Format:      SnapshotFormatBinary,
	})
	if err != nil {
		t.Fatal(err)
	}

	report, err := scheduler.RunNow()
	if err != nil {
		t.Fatalf("RunNow() error = %v", err)
	}
	if report.Manifest.SizeBytes == 0 || report.Manifest.SHA256 == "" {
		t.Fatalf("RunNow() manifest = %#v", report.Manifest)
	}
	if report.Resumed {
		t.Fatal("first RunNow() report.Resumed = true, want false")
	}
	if err := VerifySnapshotManifest(destination, report.Manifest); err != nil {
		t.Fatalf("VerifySnapshotManifest() error = %v", err)
	}
	checkpoint, err := ReadScheduledSnapshotCheckpoint(scheduler.ManifestPath())
	if err != nil {
		t.Fatalf("ReadScheduledSnapshotCheckpoint() error = %v", err)
	}
	if checkpoint.Manifest != report.Manifest {
		t.Fatalf("checkpoint manifest = %#v, want %#v", checkpoint.Manifest, report.Manifest)
	}
	if checkpoint.Destination != destination {
		t.Fatalf("checkpoint destination = %q, want %q", checkpoint.Destination, destination)
	}

	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "scheduled", Value: "after"}); !response.OK {
		t.Fatalf("second SET response = %#v", response)
	}
	second, err := scheduler.RunNow()
	if err != nil {
		t.Fatalf("second RunNow() error = %v", err)
	}
	if second.Manifest.SHA256 == report.Manifest.SHA256 {
		t.Fatalf("second snapshot digest = %q, first = %q; trie mutation was not published", second.Manifest.SHA256, report.Manifest.SHA256)
	}
	if err := VerifySnapshotManifest(destination, second.Manifest); err != nil {
		t.Fatalf("VerifySnapshotManifest(second) error = %v", err)
	}
}

func TestT213ScheduledSnapshotLoadsCheckpointAfterRestart(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "restart", Value: "value"}); !response.OK {
		t.Fatalf("SET response = %#v", response)
	}

	dir := t.TempDir()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(dir, "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	destination := filepath.Join(dir, "latest.snapshot")
	options := ScheduledSnapshotOptions{
		Interval:    time.Hour,
		Destination: destination,
		Format:      SnapshotFormatBinary,
	}
	first, err := NewScheduledSnapshotter(journal, trie, options)
	if err != nil {
		t.Fatal(err)
	}
	report, err := first.RunNow()
	if err != nil {
		t.Fatalf("first RunNow() error = %v", err)
	}

	second, err := NewScheduledSnapshotter(journal, trie, options)
	if err != nil {
		t.Fatalf("restart NewScheduledSnapshotter() error = %v", err)
	}
	status := second.Status()
	if !status.HasLast || status.Runs != 1 {
		t.Fatalf("restart status = %#v", status)
	}
	if status.Last.Manifest != report.Manifest {
		t.Fatalf("restart manifest = %#v, want %#v", status.Last.Manifest, report.Manifest)
	}
}

func TestT213ScheduledSnapshotRunsAndStops(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "scheduled", Value: "value"}); !response.OK {
		t.Fatalf("SET response = %#v", response)
	}

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	destination := filepath.Join(t.TempDir(), "latest.snapshot")
	published := make(chan ScheduledSnapshotReport, 2)
	scheduler, err := NewScheduledSnapshotter(journal, trie, ScheduledSnapshotOptions{
		Interval:    5 * time.Millisecond,
		Destination: destination,
		Format:      SnapshotFormatBinary,
		OnPublished: func(report ScheduledSnapshotReport) { published <- report },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case report := <-published:
		if report.Manifest.SizeBytes == 0 {
			t.Fatalf("published report = %#v", report)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("scheduled snapshot was not published")
	}
	if err := scheduler.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := scheduler.Stop(); !errors.Is(err, ErrScheduledSnapshotStopped) {
		t.Fatalf("second Stop() error = %v, want %v", err, ErrScheduledSnapshotStopped)
	}
	if _, err := os.Stat(destination); err != nil {
		t.Fatalf("published destination stat error = %v", err)
	}
}
