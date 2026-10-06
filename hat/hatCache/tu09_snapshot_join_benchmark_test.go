package hatCache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkTU09SnapshotJoinBaseline(b *testing.B) {
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	sourceTrie.UpsertString("base", "snapshot")
	root := b.TempDir()
	sourceSnapshot := filepath.Join(root, "source.snapshot")
	if err := sourceTrie.SaveSnapshotWithJournalSequence(sourceSnapshot, 10); err != nil {
		b.Fatal(err)
	}
	source := tu09JoinSource{snapshotPath: sourceSnapshot}
	target := CreateHatTrie()
	defer target.Destroy()
	targetJournal, err := OpenCommandJournal(filepath.Join(root, "target.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer targetJournal.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tu09ManualSnapshotJoin(source, target, targetJournal); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU09SnapshotJoinCoordinator(b *testing.B) {
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	sourceTrie.UpsertString("base", "snapshot")
	root := b.TempDir()
	sourceSnapshot := filepath.Join(root, "source.snapshot")
	if err := sourceTrie.SaveSnapshotWithJournalSequence(sourceSnapshot, 10); err != nil {
		b.Fatal(err)
	}
	source := tu09JoinSource{snapshotPath: sourceSnapshot}
	target := CreateHatTrie()
	defer target.Destroy()
	targetJournal, err := OpenCommandJournal(filepath.Join(root, "target.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer targetJournal.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := JoinFromSnapshotAndJournal(context.Background(), target, targetJournal, source, SnapshotJoinOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if result.AppliedThrough != 11 {
			b.Fatalf("unexpected applied checkpoint %d", result.AppliedThrough)
		}
	}
}

func tu09ManualSnapshotJoin(source tu09JoinSource, target *HatTrie, targetJournal *CommandJournal) error {
	stageDir, err := os.MkdirTemp("", "hatrie-tu09-manual-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stageDir)

	snapshotPath := filepath.Join(stageDir, "source.snapshot")
	sourceMetadata, err := source.PullSnapshot(context.Background(), snapshotPath, 0)
	if err != nil {
		return err
	}
	stagedTrie := CreateHatTrie()
	defer stagedTrie.Destroy()
	loadedMetadata, err := stagedTrie.LoadSnapshotWithMetadata(snapshotPath)
	if err != nil {
		return err
	}
	if loadedMetadata.JournalSequence != sourceMetadata.JournalSequence {
		return os.ErrInvalid
	}
	stagedJournal, err := OpenCommandJournal(filepath.Join(stageDir, "tail.journal"))
	if err != nil {
		return err
	}
	defer stagedJournal.Close()
	pullResult, err := source.PullJournal(context.Background(), stagedTrie, stagedJournal, loadedMetadata.JournalSequence)
	if err != nil {
		return err
	}
	appliedThrough := pullResult.AppliedThrough
	if appliedThrough == 0 {
		appliedThrough = loadedMetadata.JournalSequence
	}
	joinedSnapshotPath := filepath.Join(stageDir, "joined.snapshot")
	if err := stagedTrie.SaveSnapshotWithJournalSequence(joinedSnapshotPath, appliedThrough); err != nil {
		return err
	}
	_, err = targetJournal.ReplaceWithSnapshot(target, joinedSnapshotPath)
	return err
}
