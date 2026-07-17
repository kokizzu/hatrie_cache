package hatriecache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

const DefaultCommandJournalPullTimeout = 30 * time.Second

type CommandJournalPullOptions struct {
	Source        string
	AfterSequence uint64
	Limit         uint64
	UntilCurrent  bool
	MaxBatches    uint64
	Timeout       time.Duration
	Client        *http.Client
	DirtyTracker  *LevelDBDirtyTracker
}

type CommandJournalPullError struct {
	Status int
	Err    error
}

func (err *CommandJournalPullError) Error() string {
	if err == nil || err.Err == nil {
		return ""
	}
	return err.Err.Error()
}

func (err *CommandJournalPullError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

func PullCommandJournal(ctx context.Context, trie *HatTrie, journal *CommandJournal, options CommandJournalPullOptions) (CommandJournalPullResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if trie == nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusBadRequest, errors.New("trie is not configured"))
	}
	if journal == nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusConflict, errors.New("journal is not configured"))
	}
	limit, err := normalizeCommandJournalTailLimit(options.Limit)
	if err != nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusBadRequest, err)
	}
	maxBatches, err := normalizeCommandJournalPullBatches(options.UntilCurrent, options.MaxBatches)
	if err != nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusBadRequest, err)
	}
	client, err := commandJournalPullHTTPClient(options.Client, options.Timeout)
	if err != nil {
		return CommandJournalPullResult{}, commandJournalPullError(http.StatusBadRequest, err)
	}
	return pullCommandJournalTail(ctx, trie, journal, options.Source, options.AfterSequence, limit, options.UntilCurrent, maxBatches, client, options.DirtyTracker)
}

func commandJournalPullError(status int, err error) error {
	return &CommandJournalPullError{Status: status, Err: err}
}

func commandJournalPullHTTPClient(client *http.Client, timeout time.Duration) (*http.Client, error) {
	if timeout < 0 {
		return nil, errors.New("journal pull timeout must be non-negative")
	}
	if client != nil {
		return client, nil
	}
	if timeout == 0 {
		return http.DefaultClient, nil
	}
	return &http.Client{Timeout: timeout}, nil
}

func pullCommandJournalTail(ctx context.Context, trie *HatTrie, journal *CommandJournal, source string, afterSequence uint64, limit int, untilCurrent bool, maxBatches int, client *http.Client, dirtyTracker *LevelDBDirtyTracker) (CommandJournalPullResult, error) {
	result := CommandJournalPullResult{
		Source:         source,
		AfterSequence:  afterSequence,
		AppliedThrough: afterSequence,
	}
	for batch := 0; batch < maxBatches; batch++ {
		endpoint, err := commandJournalEndpoint(source, result.AppliedThrough, limit)
		if err != nil {
			return result, commandJournalPullError(http.StatusBadRequest, err)
		}
		tail, status, err := fetchCommandJournalTail(ctx, client, endpoint)
		if err != nil {
			return result, commandJournalPullError(status, err)
		}
		batchResult, err := applyCommandJournalTail(trie, journal, source, result.AppliedThrough, tail, dirtyTracker)
		if err != nil {
			result.LastSequence = batchResult.LastSequence
			result.CompactedThrough = batchResult.CompactedThrough
			result.Applied += batchResult.Applied
			result.AppliedThrough = batchResult.AppliedThrough
			result.HasMore = batchResult.HasMore
			result.Batches++
			status := http.StatusBadGateway
			if errors.Is(err, ErrCommandJournalCompacted) {
				status = http.StatusConflict
			}
			return result, commandJournalPullError(status, err)
		}
		result.LastSequence = batchResult.LastSequence
		result.CompactedThrough = batchResult.CompactedThrough
		result.Applied += batchResult.Applied
		result.AppliedThrough = batchResult.AppliedThrough
		result.HasMore = batchResult.HasMore
		result.Batches++
		if !untilCurrent || !result.HasMore {
			return result, nil
		}
		if batchResult.Applied == 0 {
			return result, commandJournalPullError(http.StatusBadGateway, errors.New("journal source reported more entries without returning progress"))
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
	}
	return result, nil
}

func applyCommandJournalTail(trie *HatTrie, journal *CommandJournal, source string, afterSequence uint64, tail CommandJournalTail, dirtyTrackers ...*LevelDBDirtyTracker) (CommandJournalPullResult, error) {
	var dirtyTracker *LevelDBDirtyTracker
	if len(dirtyTrackers) > 0 {
		dirtyTracker = dirtyTrackers[0]
	}
	result := CommandJournalPullResult{
		Source:           source,
		AfterSequence:    afterSequence,
		LastSequence:     tail.LastSequence,
		CompactedThrough: tail.CompactedThrough,
		AppliedThrough:   afterSequence,
		HasMore:          tail.HasMore,
	}
	if afterSequence < tail.CompactedThrough {
		return result, fmt.Errorf("%w: requested sequence %d is before compacted sequence %d", ErrCommandJournalCompacted, afterSequence, tail.CompactedThrough)
	}
	if err := validateCommandJournalTailSequences(afterSequence, tail); err != nil {
		return result, err
	}
	for _, entry := range tail.Entries {
		response := journal.ExecuteCommand(trie, entry.Request)
		if !response.OK {
			return result, fmt.Errorf("journal entry %d failed: %s", entry.Sequence, response.Message)
		}
		dirtyTracker.markCommand(entry.Request)
		result.Applied++
		result.AppliedThrough = entry.Sequence
	}
	return result, nil
}

func validateCommandJournalTailSequences(afterSequence uint64, tail CommandJournalTail) error {
	appliedThrough := afterSequence
	for _, entry := range tail.Entries {
		nextSequence, err := nextCommandJournalPullSequence(appliedThrough)
		if err != nil {
			return err
		}
		if entry.Sequence != nextSequence {
			return fmt.Errorf("journal tail sequence %d does not continue after %d", entry.Sequence, appliedThrough)
		}
		if entry.Sequence > tail.LastSequence {
			return fmt.Errorf("journal tail sequence %d exceeds last sequence %d", entry.Sequence, tail.LastSequence)
		}
		appliedThrough = entry.Sequence
	}
	if tail.HasMore {
		if len(tail.Entries) == 0 {
			return errors.New("journal tail reported more entries without returning entries")
		}
		if tail.LastSequence <= appliedThrough {
			return fmt.Errorf("journal tail reported more entries after %d but last sequence is %d", appliedThrough, tail.LastSequence)
		}
		return nil
	}
	if tail.LastSequence > appliedThrough {
		return fmt.Errorf("journal tail last sequence %d exceeds applied sequence %d without has_more", tail.LastSequence, appliedThrough)
	}
	return nil
}

func nextCommandJournalPullSequence(appliedThrough uint64) (uint64, error) {
	if appliedThrough == ^uint64(0) {
		return 0, ErrCommandJournalSequenceExhausted
	}
	return appliedThrough + 1, nil
}
