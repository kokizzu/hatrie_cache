package hatriecache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

type CommandJournalPullOptions struct {
	Source        string
	AfterSequence uint64
	Limit         uint64
	UntilCurrent  bool
	MaxBatches    uint64
	Client        *http.Client
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
	return pullCommandJournalTail(ctx, trie, journal, options.Source, options.AfterSequence, limit, options.UntilCurrent, maxBatches, options.Client)
}

func commandJournalPullError(status int, err error) error {
	return &CommandJournalPullError{Status: status, Err: err}
}

func pullCommandJournalTail(ctx context.Context, trie *HatTrie, journal *CommandJournal, source string, afterSequence uint64, limit int, untilCurrent bool, maxBatches int, client *http.Client) (CommandJournalPullResult, error) {
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
		batchResult, err := applyCommandJournalTail(trie, journal, source, result.AppliedThrough, tail)
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

func applyCommandJournalTail(trie *HatTrie, journal *CommandJournal, source string, afterSequence uint64, tail CommandJournalTail) (CommandJournalPullResult, error) {
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
	for _, entry := range tail.Entries {
		nextSequence := result.AppliedThrough + 1
		if entry.Sequence != nextSequence {
			return result, fmt.Errorf("journal tail sequence %d does not continue after %d", entry.Sequence, result.AppliedThrough)
		}
		response := journal.ExecuteCommand(trie, entry.Request)
		if !response.OK {
			return result, fmt.Errorf("journal entry %d failed: %s", entry.Sequence, response.Message)
		}
		result.Applied++
		result.AppliedThrough = entry.Sequence
	}
	return result, nil
}
