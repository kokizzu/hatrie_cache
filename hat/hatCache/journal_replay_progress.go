package hatCache

import (
	"sync"
	"time"
)

// CommandJournalReplayProgress reports the most recent opt-in journal replay.
// While Active is true, callers can poll ReplayProgress from another
// goroutine. ETA is a best-effort estimate based on completed entries.
type CommandJournalReplayProgress struct {
	Active             bool          `json:"active"`
	Total              uint64        `json:"total"`
	Applied            uint64        `json:"applied"`
	CurrentSequence    uint64        `json:"current_sequence"`
	StartedAt          time.Time     `json:"started_at,omitempty"`
	FinishedAt         time.Time     `json:"finished_at,omitempty"`
	Elapsed            time.Duration `json:"elapsed"`
	EstimatedRemaining time.Duration `json:"estimated_remaining"`
	Error              string        `json:"error,omitempty"`
}

type commandJournalReplayProgressState struct {
	mu       sync.RWMutex
	progress CommandJournalReplayProgress
}

func newCommandJournalReplayProgress(afterSequence uint64) *commandJournalReplayProgressState {
	return &commandJournalReplayProgressState{
		progress: CommandJournalReplayProgress{
			Active:          true,
			CurrentSequence: afterSequence,
			StartedAt:       time.Now().UTC(),
		},
	}
}

func (state *commandJournalReplayProgressState) setTotal(total uint64) {
	state.mu.Lock()
	state.progress.Total = total
	state.mu.Unlock()
}

func (state *commandJournalReplayProgressState) markCurrent(sequence uint64) {
	state.mu.Lock()
	state.progress.CurrentSequence = sequence
	state.mu.Unlock()
}

func (state *commandJournalReplayProgressState) markApplied(sequence uint64) {
	state.mu.Lock()
	state.progress.Applied++
	state.progress.CurrentSequence = sequence
	state.mu.Unlock()
}

func (state *commandJournalReplayProgressState) finish(replayErr error) {
	now := time.Now().UTC()
	state.mu.Lock()
	state.progress.Active = false
	state.progress.FinishedAt = now
	state.progress.Elapsed = now.Sub(state.progress.StartedAt)
	state.progress.EstimatedRemaining = 0
	if replayErr != nil {
		state.progress.Error = replayErr.Error()
	}
	state.mu.Unlock()
}

func (state *commandJournalReplayProgressState) snapshot() CommandJournalReplayProgress {
	state.mu.RLock()
	progress := state.progress
	state.mu.RUnlock()
	now := time.Now().UTC()
	if progress.Active {
		progress.Elapsed = now.Sub(progress.StartedAt)
		progress.EstimatedRemaining = estimateCommandJournalReplayRemaining(progress.Elapsed, progress.Applied, progress.Total)
	}
	return progress
}

func estimateCommandJournalReplayRemaining(elapsed time.Duration, applied uint64, total uint64) time.Duration {
	if elapsed <= 0 || applied == 0 || applied >= total {
		return 0
	}
	remaining := float64(elapsed) * float64(total-applied) / float64(applied)
	if remaining <= 0 {
		return 0
	}
	return time.Duration(remaining)
}
