package hatCache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func tu09RequireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func tu09RequireEqual[T comparable](t *testing.T, expected, actual T) {
	t.Helper()
	if expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}

func tu09RequireFalse(t *testing.T, value bool) {
	t.Helper()
	if value {
		t.Fatal("expected false")
	}
}

func tu09RequireErrorContains(t *testing.T, err error, message string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), message) {
		t.Fatalf("expected error containing %q, got %v", message, err)
	}
}

type tu09JoinSource struct {
	snapshotPath     string
	pullErr          error
	snapshotMetadata *SnapshotMetadata
	pullResult       *CommandJournalPullResult
	stagePath        *string
}

func (source tu09JoinSource) PullSnapshot(_ context.Context, path string, _ uint64) (SnapshotMetadata, error) {
	data, err := os.ReadFile(source.snapshotPath)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return SnapshotMetadata{}, err
	}
	if source.stagePath != nil {
		*source.stagePath = path
	}
	if source.snapshotMetadata != nil {
		return *source.snapshotMetadata, nil
	}
	return ReadSnapshotMetadata(path)
}

func (source tu09JoinSource) PullJournal(_ context.Context, trie *HatTrie, journal *CommandJournal, afterSequence uint64) (CommandJournalPullResult, error) {
	if source.pullErr != nil {
		return CommandJournalPullResult{}, source.pullErr
	}
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "tail", Value: "journal"})
	if !response.OK {
		return CommandJournalPullResult{}, errors.New("journal command failed")
	}
	if source.pullResult != nil {
		return *source.pullResult, nil
	}
	return CommandJournalPullResult{
		AfterSequence:  afterSequence,
		LastSequence:   afterSequence + 1,
		Applied:        1,
		AppliedThrough: afterSequence + 1,
		Batches:        1,
	}, nil
}

func TestJoinFromSnapshotAndJournalActivatesFencedSnapshotAndTail(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	sourceTrie.UpsertString("base", "snapshot")
	sourceSnapshot := filepath.Join(root, "source.snapshot")
	tu09RequireNoError(t, sourceTrie.SaveSnapshotWithJournalSequence(sourceSnapshot, 10))
	var stagePath string

	target := CreateHatTrie()
	defer target.Destroy()
	target.UpsertString("old", "must-disappear")
	targetJournal, err := OpenCommandJournal(filepath.Join(root, "target.journal"))
	tu09RequireNoError(t, err)
	defer targetJournal.Close()

	var validatedFence uint64
	result, err := JoinFromSnapshotAndJournal(ctx, target, targetJournal, tu09JoinSource{
		snapshotPath: sourceSnapshot,
		stagePath:    &stagePath,
	}, SnapshotJoinOptions{
		FencingToken: 42,
		ValidateFence: func(_ context.Context, token uint64) error {
			validatedFence = token
			return nil
		},
	})
	tu09RequireNoError(t, err)
	tu09RequireEqual(t, uint64(10), result.SnapshotSequence)
	tu09RequireEqual(t, uint64(11), result.AppliedThrough)
	tu09RequireEqual(t, 1, result.Applied)
	tu09RequireEqual(t, 1, result.Batches)
	tu09RequireEqual(t, uint64(42), result.FencingToken)
	tu09RequireEqual(t, uint64(42), validatedFence)
	tu09RequireEqual(t, "snapshot", target.GetString("base"))
	tu09RequireEqual(t, "journal", target.GetString("tail"))
	tu09RequireFalse(t, target.Exists("old"))
	tu09RequireEqual(t, uint64(11), targetJournal.Sequence())
	if _, err := os.Stat(stagePath); !os.IsNotExist(err) {
		t.Fatalf("expected staged snapshot path to be removed, stat error: %v", err)
	}
}

func TestJoinFromSnapshotAndJournalKeepsLiveStateWhenPullFails(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	sourceTrie.UpsertString("base", "snapshot")
	sourceSnapshot := filepath.Join(root, "source.snapshot")
	tu09RequireNoError(t, sourceTrie.SaveSnapshotWithJournalSequence(sourceSnapshot, 10))

	target := CreateHatTrie()
	defer target.Destroy()
	target.UpsertString("old", "keep")
	targetJournal, err := OpenCommandJournal(filepath.Join(root, "target.journal"))
	tu09RequireNoError(t, err)
	defer targetJournal.Close()

	_, err = JoinFromSnapshotAndJournal(context.Background(), target, targetJournal, tu09JoinSource{
		snapshotPath: sourceSnapshot,
		pullErr:      errors.New("journal pull failed"),
	}, SnapshotJoinOptions{})
	tu09RequireErrorContains(t, err, "journal pull failed")
	tu09RequireEqual(t, "keep", target.GetString("old"))
	tu09RequireFalse(t, target.Exists("base"))
}

func TestJoinFromSnapshotAndJournalRequiresFenceValidation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	sourceSnapshot := filepath.Join(root, "source.snapshot")
	tu09RequireNoError(t, sourceTrie.SaveSnapshotWithJournalSequence(sourceSnapshot, 1))
	targetJournal, err := OpenCommandJournal(filepath.Join(root, "target.journal"))
	tu09RequireNoError(t, err)
	defer targetJournal.Close()

	target := CreateHatTrie()
	defer target.Destroy()
	_, err = JoinFromSnapshotAndJournal(context.Background(), target, targetJournal, tu09JoinSource{
		snapshotPath: sourceSnapshot,
	}, SnapshotJoinOptions{FencingToken: 1})
	tu09RequireErrorContains(t, err, "fence validator")
}

func TestJoinFromSnapshotAndJournalRejectsMetadataMismatchWithoutMutation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	sourceTrie.UpsertString("base", "snapshot")
	sourceSnapshot := filepath.Join(root, "source.snapshot")
	tu09RequireNoError(t, sourceTrie.SaveSnapshotWithJournalSequence(sourceSnapshot, 10))
	wrongMetadata := SnapshotMetadata{JournalSequence: 9}
	var stagePath string

	target := CreateHatTrie()
	defer target.Destroy()
	target.UpsertString("old", "keep")
	targetJournal, err := OpenCommandJournal(filepath.Join(root, "target.journal"))
	tu09RequireNoError(t, err)
	defer targetJournal.Close()

	_, err = JoinFromSnapshotAndJournal(context.Background(), target, targetJournal, tu09JoinSource{
		snapshotPath:     sourceSnapshot,
		snapshotMetadata: &wrongMetadata,
		stagePath:        &stagePath,
	}, SnapshotJoinOptions{})
	tu09RequireErrorContains(t, err, "metadata mismatch")
	tu09RequireEqual(t, "keep", target.GetString("old"))
	tu09RequireFalse(t, target.Exists("base"))
	if _, statErr := os.Stat(stagePath); !os.IsNotExist(statErr) {
		t.Fatalf("expected staged mismatch path to be removed, stat error: %v", statErr)
	}
}

func TestJoinFromSnapshotAndJournalRejectsJournalRegressionWithoutMutation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	sourceSnapshot := filepath.Join(root, "source.snapshot")
	tu09RequireNoError(t, sourceTrie.SaveSnapshotWithJournalSequence(sourceSnapshot, 10))
	regressed := CommandJournalPullResult{
		AfterSequence:  10,
		LastSequence:   10,
		Applied:        1,
		AppliedThrough: 9,
		Batches:        1,
	}

	target := CreateHatTrie()
	defer target.Destroy()
	target.UpsertString("old", "keep")
	targetJournal, err := OpenCommandJournal(filepath.Join(root, "target.journal"))
	tu09RequireNoError(t, err)
	defer targetJournal.Close()

	_, err = JoinFromSnapshotAndJournal(context.Background(), target, targetJournal, tu09JoinSource{
		snapshotPath: sourceSnapshot,
		pullResult:   &regressed,
	}, SnapshotJoinOptions{})
	tu09RequireErrorContains(t, err, "regressed checkpoint")
	tu09RequireEqual(t, "keep", target.GetString("old"))
	tu09RequireFalse(t, target.Exists("base"))
}

func TestJoinFromSnapshotAndJournalHonorsCanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	target := CreateHatTrie()
	defer target.Destroy()
	root := t.TempDir()
	targetJournal, err := OpenCommandJournal(filepath.Join(root, "target.journal"))
	tu09RequireNoError(t, err)
	defer targetJournal.Close()

	_, err = JoinFromSnapshotAndJournal(ctx, target, targetJournal, tu09JoinSource{}, SnapshotJoinOptions{})
	tu09RequireErrorContains(t, err, "canceled")
}
