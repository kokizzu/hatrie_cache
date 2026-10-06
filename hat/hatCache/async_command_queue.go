package hatCache

import (
	"context"
)

// AsyncCommandQueueStats is a privacy-preserving snapshot of the journal's
// bounded asynchronous write queue. It contains counters and queue shape, but
// never retains or returns command keys, values, or idempotency tokens.
type AsyncCommandQueueStats struct {
	Enabled    bool   `json:"enabled"`
	Capacity   int    `json:"capacity"`
	QueueDepth int    `json:"queue_depth"`
	Pending    int    `json:"pending"`
	Accepted   uint64 `json:"accepted"`
	Completed  uint64 `json:"completed"`
	Rejected   uint64 `json:"rejected"`
	Failed     uint64 `json:"failed"`
}

type asyncCommandQueueCounters struct {
	accepted  uint64
	completed uint64
	rejected  uint64
	failed    uint64
}

// AsyncCommandQueueStats returns a bounded snapshot of async command
// admission. Pending includes commands currently being synced or applied, so
// it is the value an operator should use to determine whether a flush is
// still in progress. A journal opened without group commit reports Enabled
// false and a zero capacity.
func (journal *CommandJournal) AsyncCommandQueueStats() AsyncCommandQueueStats {
	if journal == nil {
		return AsyncCommandQueueStats{}
	}
	journal.asyncCommandMu.Lock()
	stats := AsyncCommandQueueStats{
		Enabled:    journal.groupCommitEnabled(),
		Capacity:   journal.groupCommitMaxBatch,
		QueueDepth: len(journal.groupCommitJobs),
		Pending:    len(journal.asyncCommandSubmissions),
		Accepted:   journal.asyncCommandCounters.accepted,
		Completed:  journal.asyncCommandCounters.completed,
		Rejected:   journal.asyncCommandCounters.rejected,
		Failed:     journal.asyncCommandCounters.failed,
	}
	if !stats.Enabled {
		stats.Capacity = 0
		stats.QueueDepth = 0
	}
	journal.asyncCommandMu.Unlock()
	return stats
}

// FlushAsyncCommands waits for every async command admitted before the flush
// barrier. Commands admitted after the barrier are intentionally left for a
// later flush. Cancellation only cancels this wait; it never cancels an
// admitted journal write.
func (journal *CommandJournal) FlushAsyncCommands(ctx context.Context) error {
	if journal == nil {
		return ErrNilCommandJournal
	}
	if !journal.groupCommitEnabled() {
		return ErrCommandJournalAsyncUnsupported
	}

	journal.submitMu.Lock()
	journal.asyncCommandMu.Lock()
	pending := make([]*CommandJournalSubmission, 0, len(journal.asyncCommandSubmissions))
	for submission := range journal.asyncCommandSubmissions {
		pending = append(pending, submission)
	}
	journal.asyncCommandMu.Unlock()
	journal.submitMu.Unlock()

	for _, submission := range pending {
		if _, err := submission.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (journal *CommandJournal) tryAdmitAsyncCommand(submission *CommandJournalSubmission, job *commandJournalJob) bool {
	journal.asyncCommandMu.Lock()
	if journal.asyncCommandSubmissions == nil {
		journal.asyncCommandSubmissions = make(map[*CommandJournalSubmission]struct{}, journal.groupCommitMaxBatch)
	}
	journal.asyncCommandSubmissions[submission] = struct{}{}
	select {
	case journal.groupCommitJobs <- job:
		journal.asyncCommandCounters.accepted++
		journal.asyncCommandMu.Unlock()
		return true
	default:
		delete(journal.asyncCommandSubmissions, submission)
		journal.asyncCommandMu.Unlock()
		return false
	}
}

func (journal *CommandJournal) completeAsyncCommand(submission *CommandJournalSubmission, status AsyncCommandSubmissionStatus) {
	journal.asyncCommandMu.Lock()
	delete(journal.asyncCommandSubmissions, submission)
	switch status {
	case AsyncCommandSubmissionCompleted:
		journal.asyncCommandCounters.completed++
	case AsyncCommandSubmissionRejected:
		journal.asyncCommandCounters.rejected++
	case AsyncCommandSubmissionFailed:
		journal.asyncCommandCounters.failed++
	}
	journal.asyncCommandMu.Unlock()
}
