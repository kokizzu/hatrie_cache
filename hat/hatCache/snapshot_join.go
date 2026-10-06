package hatCache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

// CommandJournalJoinSource provides the two source operations needed to build
// a consistent snapshot-plus-journal join without mutating the live trie.
type CommandJournalJoinSource interface {
	PullSnapshot(context.Context, string, uint64) (SnapshotMetadata, error)
	PullJournal(context.Context, *HatTrie, *CommandJournal, uint64) (CommandJournalPullResult, error)
}

// SnapshotJoinOptions controls snapshot-plus-journal bootstrap behavior.
type SnapshotJoinOptions struct {
	// MinimumSequence rejects snapshots older than this checkpoint.
	MinimumSequence uint64
	// FencingToken is returned to the fence validator and in the result.
	FencingToken uint64
	// ValidateFence must be supplied when FencingToken is non-zero. It is
	// called immediately before the staged state is activated.
	ValidateFence func(context.Context, uint64) error
	// SnapshotFormat selects the staged output format. The zero value uses the
	// repository default.
	SnapshotFormat SnapshotFormat
}

// SnapshotJoinResult describes the checkpoint activated by a successful join.
type SnapshotJoinResult struct {
	SnapshotSequence uint64
	AppliedThrough   uint64
	LastSequence     uint64
	Applied          int
	Batches          int
	FencingToken     uint64
}

// HTTPCommandJournalJoinSource adapts the existing HTTP snapshot and journal
// pull APIs to CommandJournalJoinSource.
type HTTPCommandJournalJoinSource struct {
	Source    string
	AuthToken string
	Client    *http.Client
	Pull      CommandJournalPullOptions
}

func (source HTTPCommandJournalJoinSource) PullSnapshot(ctx context.Context, path string, minimumSequence uint64) (SnapshotMetadata, error) {
	return PullCommandJournalSnapshot(ctx, source.Source, source.AuthToken, source.Client, path, minimumSequence)
}

func (source HTTPCommandJournalJoinSource) PullJournal(ctx context.Context, trie *HatTrie, journal *CommandJournal, afterSequence uint64) (CommandJournalPullResult, error) {
	options := source.Pull
	options.Source = source.Source
	options.AuthToken = source.AuthToken
	options.Client = source.Client
	options.AfterSequence = afterSequence
	if options.Limit == 0 && options.MaxBatches == 0 {
		options.UntilCurrent = true
	}
	return PullCommandJournal(ctx, trie, journal, options)
}

// JoinFromSnapshotAndJournal stages a source snapshot and its journal tail,
// then atomically replaces the live trie only after both have validated.
func JoinFromSnapshotAndJournal(
	ctx context.Context,
	target *HatTrie,
	targetJournal *CommandJournal,
	source CommandJournalJoinSource,
	options SnapshotJoinOptions,
) (SnapshotJoinResult, error) {
	if ctx == nil {
		return SnapshotJoinResult{}, errors.New("hatriecache: snapshot join context is nil")
	}
	if target == nil {
		return SnapshotJoinResult{}, ErrNilHatTrie
	}
	if targetJournal == nil {
		return SnapshotJoinResult{}, ErrNilCommandJournal
	}
	if source == nil {
		return SnapshotJoinResult{}, errors.New("hatriecache: snapshot join source is nil")
	}
	if options.FencingToken != 0 && options.ValidateFence == nil {
		return SnapshotJoinResult{}, errors.New("hatriecache: snapshot join fence validator is required")
	}
	if err := ctx.Err(); err != nil {
		return SnapshotJoinResult{}, err
	}

	format := options.SnapshotFormat
	if format == "" {
		format = DefaultSnapshotFormat
	}
	format, err := ParseSnapshotFormat(string(format))
	if err != nil {
		return SnapshotJoinResult{}, err
	}

	stageDir, err := os.MkdirTemp("", "hatrie-snapshot-join-")
	if err != nil {
		return SnapshotJoinResult{}, err
	}
	defer os.RemoveAll(stageDir)

	stagedSnapshotPath := filepath.Join(stageDir, "source.snapshot")
	sourceMetadata, err := source.PullSnapshot(ctx, stagedSnapshotPath, options.MinimumSequence)
	if err != nil {
		return SnapshotJoinResult{}, fmt.Errorf("hatriecache: pull join snapshot: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return SnapshotJoinResult{}, err
	}

	stagedTrie := CreateHatTrie()
	defer stagedTrie.Destroy()
	loadedMetadata, err := stagedTrie.LoadSnapshotWithMetadata(stagedSnapshotPath)
	if err != nil {
		return SnapshotJoinResult{}, fmt.Errorf("hatriecache: load join snapshot: %w", err)
	}
	if loadedMetadata.JournalSequence != sourceMetadata.JournalSequence {
		return SnapshotJoinResult{}, fmt.Errorf(
			"hatriecache: join snapshot metadata mismatch: source=%d loaded=%d",
			sourceMetadata.JournalSequence,
			loadedMetadata.JournalSequence,
		)
	}
	if loadedMetadata.JournalSequence < options.MinimumSequence {
		return SnapshotJoinResult{}, fmt.Errorf(
			"hatriecache: join snapshot sequence %d is below minimum %d",
			loadedMetadata.JournalSequence,
			options.MinimumSequence,
		)
	}

	stagedJournal, err := OpenCommandJournal(filepath.Join(stageDir, "tail.journal"))
	if err != nil {
		return SnapshotJoinResult{}, fmt.Errorf("hatriecache: open staged join journal: %w", err)
	}
	defer stagedJournal.Close()

	pullResult, err := source.PullJournal(ctx, stagedTrie, stagedJournal, loadedMetadata.JournalSequence)
	if err != nil {
		return SnapshotJoinResult{}, fmt.Errorf("hatriecache: pull join journal: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return SnapshotJoinResult{}, err
	}
	if pullResult.AfterSequence != loadedMetadata.JournalSequence {
		return SnapshotJoinResult{}, fmt.Errorf(
			"hatriecache: join journal starts at %d, expected %d",
			pullResult.AfterSequence,
			loadedMetadata.JournalSequence,
		)
	}
	if pullResult.Applied < 0 || pullResult.Batches < 0 {
		return SnapshotJoinResult{}, errors.New("hatriecache: join journal returned negative counts")
	}
	appliedThrough := loadedMetadata.JournalSequence
	if pullResult.AppliedThrough != 0 {
		if pullResult.AppliedThrough < appliedThrough {
			return SnapshotJoinResult{}, fmt.Errorf(
				"hatriecache: join journal regressed checkpoint from %d to %d",
				appliedThrough,
				pullResult.AppliedThrough,
			)
		}
		appliedThrough = pullResult.AppliedThrough
	}
	if pullResult.Applied == 0 && appliedThrough != loadedMetadata.JournalSequence {
		return SnapshotJoinResult{}, errors.New("hatriecache: join journal advanced without applied records")
	}
	if pullResult.LastSequence != 0 && pullResult.LastSequence < appliedThrough {
		return SnapshotJoinResult{}, fmt.Errorf(
			"hatriecache: join journal last sequence %d is below applied checkpoint %d",
			pullResult.LastSequence,
			appliedThrough,
		)
	}

	joinedSnapshotPath := filepath.Join(stageDir, "joined.snapshot")
	if err := stagedTrie.SaveSnapshotWithJournalSequenceAndFormat(joinedSnapshotPath, appliedThrough, format); err != nil {
		return SnapshotJoinResult{}, fmt.Errorf("hatriecache: write joined snapshot: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return SnapshotJoinResult{}, err
	}
	if options.ValidateFence != nil {
		if err := options.ValidateFence(ctx, options.FencingToken); err != nil {
			return SnapshotJoinResult{}, fmt.Errorf("hatriecache: validate snapshot join fence: %w", err)
		}
	}
	activatedMetadata, err := targetJournal.ReplaceWithSnapshot(target, joinedSnapshotPath)
	if err != nil {
		return SnapshotJoinResult{}, fmt.Errorf("hatriecache: activate joined snapshot: %w", err)
	}
	if activatedMetadata.JournalSequence != appliedThrough {
		return SnapshotJoinResult{}, fmt.Errorf(
			"hatriecache: activated snapshot sequence %d, expected %d",
			activatedMetadata.JournalSequence,
			appliedThrough,
		)
	}

	return SnapshotJoinResult{
		SnapshotSequence: loadedMetadata.JournalSequence,
		AppliedThrough:   appliedThrough,
		LastSequence:     pullResult.LastSequence,
		Applied:          pullResult.Applied,
		Batches:          pullResult.Batches,
		FencingToken:     options.FencingToken,
	}, nil
}
