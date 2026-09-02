package hatCache

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrCommandJournalAsyncUnsupported reports that asynchronous submission is
	// unavailable because the journal was opened without a group-commit worker.
	ErrCommandJournalAsyncUnsupported = errors.New("hatriecache: async command submission requires group commit")
	// ErrCommandJournalAsyncWriteOnly reports that the request is not a journaled
	// write and therefore cannot be submitted to the async write queue.
	ErrCommandJournalAsyncWriteOnly = errors.New("hatriecache: async command submission requires a journaled write")
	// ErrCommandJournalAsyncQueueFull reports bounded queue backpressure.
	ErrCommandJournalAsyncQueueFull = errors.New("hatriecache: async command submission queue is full")
	// ErrNilCommandJournalSubmission reports a nil submission handle.
	ErrNilCommandJournalSubmission = errors.New("hatriecache: command journal submission is nil")
)

// AsyncCommandSubmissionStatus describes whether a submitted command has
// reached its durable-and-applied completion point.
type AsyncCommandSubmissionStatus uint8

const (
	AsyncCommandSubmissionUnknown AsyncCommandSubmissionStatus = iota
	AsyncCommandSubmissionPending
	AsyncCommandSubmissionCompleted
)

func (status AsyncCommandSubmissionStatus) String() string {
	switch status {
	case AsyncCommandSubmissionPending:
		return "pending"
	case AsyncCommandSubmissionCompleted:
		return "completed"
	default:
		return "unknown"
	}
}

// CommandJournalSubmission is a repeatable future for one asynchronously
// submitted journaled command. Completion means the command was fsynced and
// then applied to the in-memory trie; command rejection is returned in the
// response, while ctx cancellation is returned as a Go error.
type CommandJournalSubmission struct {
	done chan struct{}

	mu        sync.Mutex
	completed bool
	response  CacheCommandResponse
}

func newCommandJournalSubmission() *CommandJournalSubmission {
	return &CommandJournalSubmission{done: make(chan struct{})}
}

// Done returns a channel closed at the durable-and-applied completion point.
func (submission *CommandJournalSubmission) Done() <-chan struct{} {
	if submission == nil {
		return nil
	}
	return submission.done
}

// Status returns the current completion state without waiting.
func (submission *CommandJournalSubmission) Status() AsyncCommandSubmissionStatus {
	if submission == nil {
		return AsyncCommandSubmissionUnknown
	}
	select {
	case <-submission.done:
		return AsyncCommandSubmissionCompleted
	default:
		return AsyncCommandSubmissionPending
	}
}

// Wait waits for durable-and-applied completion. A nil context is treated as
// context.Background for convenience; callers can use a deadline to bound the
// wait without cancelling the queued command itself.
func (submission *CommandJournalSubmission) Wait(ctx context.Context) (CacheCommandResponse, error) {
	if submission == nil {
		return CacheCommandResponse{}, ErrNilCommandJournalSubmission
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-submission.done:
		submission.mu.Lock()
		response := cloneCacheCommandResponse(submission.response)
		submission.mu.Unlock()
		return response, nil
	case <-ctx.Done():
		return CacheCommandResponse{}, ctx.Err()
	}
}

func (submission *CommandJournalSubmission) complete(response CacheCommandResponse) {
	submission.mu.Lock()
	if submission.completed {
		submission.mu.Unlock()
		return
	}
	submission.response = cloneCacheCommandResponse(response)
	submission.completed = true
	close(submission.done)
	submission.mu.Unlock()
}

// SubmitAsyncCommand admits one journaled write without waiting for the
// journal's group-commit worker. The returned submission becomes complete only
// after journal sync and in-memory application. Admission is bounded by the
// configured GroupCommitMaxBatch channel capacity and returns immediately when
// that queue is full.
func (journal *CommandJournal) SubmitAsyncCommand(trie *HatTrie, request CacheCommandRequest) (*CommandJournalSubmission, error) {
	return journal.submitAsyncCommand(trie, request, nil)
}

func (journal *CommandJournal) submitAsyncCommand(trie *HatTrie, request CacheCommandRequest, onComplete func(CacheCommandResponse)) (*CommandJournalSubmission, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if trie == nil {
		return nil, ErrNilHatTrie
	}
	request.Command = normalizedCommand(request.Command)
	if !commandShouldJournal(request) {
		return nil, ErrCommandJournalAsyncWriteOnly
	}
	if !journal.groupCommitEnabled() {
		return nil, ErrCommandJournalAsyncUnsupported
	}

	request = cloneAsyncCommandRequest(request)
	journalRequest := journal.normalizeJournalRequest(request, trie.currentTime())
	check, err := journal.idempotencyCheck(request)
	if err != nil {
		return nil, err
	}
	submission := newCommandJournalSubmission()
	job := &commandJournalJob{
		trie:           trie,
		request:        request,
		journalRequest: journalRequest,
		idempotency:    check,
		submission:     submission,
		onComplete:     onComplete,
	}

	journal.submitMu.RLock()
	defer journal.submitMu.RUnlock()
	if !journal.accepting {
		return nil, ErrCommandJournalClosed
	}
	select {
	case journal.groupCommitJobs <- job:
		return submission, nil
	default:
		return nil, ErrCommandJournalAsyncQueueFull
	}
}

func (job *commandJournalJob) complete(response CacheCommandResponse) {
	if job.onComplete != nil {
		job.onComplete(response)
	}
	if job.submission != nil {
		job.submission.complete(response)
		return
	}
	job.result <- response
}

func cloneAsyncCommandRequest(request CacheCommandRequest) CacheCommandRequest {
	out := request
	out.Values = cloneAsyncCommandValues(request.Values)
	if request.Batch != nil {
		out.Batch = make([]CacheCommandRequest, len(request.Batch))
		for index, nested := range request.Batch {
			out.Batch[index] = cloneAsyncCommandRequest(nested)
		}
	}
	if request.Pairs != nil {
		out.Pairs = make(map[string]any, len(request.Pairs))
		for key, value := range request.Pairs {
			out.Pairs[key] = cloneAsyncCommandValue(value)
		}
	}
	if request.Priority != nil {
		priority := *request.Priority
		out.Priority = &priority
	}
	if request.TTLSeconds != nil {
		ttlSeconds := *request.TTLSeconds
		out.TTLSeconds = &ttlSeconds
	}
	if request.UnixSeconds != nil {
		unixSeconds := *request.UnixSeconds
		out.UnixSeconds = &unixSeconds
	}
	out.BinaryValue = append([]byte(nil), request.BinaryValue...)
	return out
}

func cloneAsyncCommandValues(values []any) []any {
	if values == nil {
		return nil
	}
	out := make([]any, len(values))
	for index, value := range values {
		out[index] = cloneAsyncCommandValue(value)
	}
	return out
}

func cloneAsyncCommandValue(value any) any {
	switch value := value.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case []any:
		return cloneAsyncCommandValues(value)
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, nested := range value {
			out[key] = cloneAsyncCommandValue(nested)
		}
		return out
	default:
		return value
	}
}
