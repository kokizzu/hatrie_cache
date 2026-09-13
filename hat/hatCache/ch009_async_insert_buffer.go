package hatCache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultAsyncInsertBatchSize bounds one durable batch while still
	// amortizing journal and command-dispatch overhead.
	DefaultAsyncInsertBatchSize = 64
	// DefaultAsyncInsertBufferCapacity bounds caller-owned requests waiting for
	// the background flusher.
	DefaultAsyncInsertBufferCapacity = 4096
	// DefaultAsyncInsertFlushInterval bounds visibility latency for partial
	// batches. A zero interval in options selects this value.
	DefaultAsyncInsertFlushInterval = 5 * time.Millisecond
)

var (
	ErrAsyncInsertBufferWriteOnly = errors.New("hatriecache: async insert buffer accepts journaled writes only")
	ErrAsyncInsertBufferFull      = errors.New("hatriecache: async insert buffer is full")
	ErrAsyncInsertBufferClosed    = errors.New("hatriecache: async insert buffer is closed")
	ErrNilAsyncInsertSubmission   = errors.New("hatriecache: async insert submission is nil")
)

const (
	AsyncInsertSubmissionUnknown   = AsyncCommandSubmissionUnknown
	AsyncInsertSubmissionPending   = AsyncCommandSubmissionPending
	AsyncInsertSubmissionCompleted = AsyncCommandSubmissionCompleted
)

// AsyncInsertBufferOptions controls the opt-in asynchronous insert buffer.
// The buffer is disabled unless NewAsyncInsertBuffer is called explicitly.
// BatchSize is the number of write commands submitted during one flush;
// Capacity is the maximum number of queued commands, excluding the batch
// currently being durably processed.
type AsyncInsertBufferOptions struct {
	BatchSize     int
	Capacity      int
	FlushInterval time.Duration
}

type asyncInsertBatch struct {
	done chan struct{}

	mu           sync.Mutex
	completed    bool
	response     CacheCommandResponse
	err          error
	requestCount int
	requests     []CacheCommandRequest
}

// AsyncInsertSubmission is a repeatable future for one buffered write. Wait
// returns the response corresponding to the submitted command, after its
// containing batch is durably appended and applied to the trie.
type AsyncInsertSubmission struct {
	batch *asyncInsertBatch
	index int
}

// AsyncInsertBuffer batches journaled writes into bounded flush groups. It
// owns copies of requests after Submit returns and has one background flusher,
// preserving submission order while letting the journal group commit the
// individual records together.
type AsyncInsertBuffer struct {
	journal *CommandJournal
	trie    *HatTrie
	options AsyncInsertBufferOptions

	mu           sync.Mutex
	current      *asyncInsertBatch
	ready        []*asyncInsertBatch
	queued       int
	closed       bool
	closeErr     error
	closeOnce    sync.Once
	wake         chan struct{}
	flushRequest chan chan error
	close        chan struct{}
	done         chan struct{}
}

// NewAsyncInsertBuffer creates an opt-in bounded writer. It requires a
// CommandJournal with group commit enabled because the buffer submits each
// flush group without blocking the caller.
func NewAsyncInsertBuffer(journal *CommandJournal, trie *HatTrie, options AsyncInsertBufferOptions) (*AsyncInsertBuffer, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	if trie == nil {
		return nil, ErrNilHatTrie
	}
	if !journal.groupCommitEnabled() {
		return nil, ErrCommandJournalAsyncUnsupported
	}
	normalized, err := normalizeAsyncInsertBufferOptions(options)
	if err != nil {
		return nil, err
	}
	buffer := &AsyncInsertBuffer{
		journal:      journal,
		trie:         trie,
		options:      normalized,
		ready:        make([]*asyncInsertBatch, 0, (normalized.Capacity+normalized.BatchSize-1)/normalized.BatchSize),
		wake:         make(chan struct{}, 1),
		flushRequest: make(chan chan error, 1),
		close:        make(chan struct{}),
		done:         make(chan struct{}),
	}
	go buffer.run()
	return buffer, nil
}

func normalizeAsyncInsertBufferOptions(options AsyncInsertBufferOptions) (AsyncInsertBufferOptions, error) {
	if options.BatchSize == 0 {
		options.BatchSize = DefaultAsyncInsertBatchSize
	}
	if options.Capacity == 0 {
		options.Capacity = DefaultAsyncInsertBufferCapacity
	}
	if options.FlushInterval == 0 {
		options.FlushInterval = DefaultAsyncInsertFlushInterval
	}
	if options.BatchSize < 1 || options.BatchSize > maxPublicCommandBatchSize {
		return AsyncInsertBufferOptions{}, fmt.Errorf("hatriecache: async insert batch size must be between 1 and %d", maxPublicCommandBatchSize)
	}
	if options.Capacity < 1 || options.Capacity > maxPublicCommandBatchSize*16 {
		return AsyncInsertBufferOptions{}, fmt.Errorf("hatriecache: async insert capacity must be between 1 and %d", maxPublicCommandBatchSize*16)
	}
	if options.FlushInterval < 0 {
		return AsyncInsertBufferOptions{}, errors.New("hatriecache: async insert flush interval cannot be negative")
	}
	return options, nil
}

// Submit admits one journaled write and returns immediately. Admission is
// bounded; a full buffer returns ErrAsyncInsertBufferFull rather than
// silently allocating unbounded memory. A caller context canceled before
// admission is honored, but cancellation after admission does not cancel the
// durable write.
func (buffer *AsyncInsertBuffer) Submit(ctx context.Context, request CacheCommandRequest) (*AsyncInsertSubmission, error) {
	if buffer == nil {
		return nil, ErrAsyncInsertBufferClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateAsyncInsertRequest(request); err != nil {
		return nil, err
	}
	request = cloneAsyncCommandRequest(request)

	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if buffer.closed {
		return nil, ErrAsyncInsertBufferClosed
	}
	if buffer.queued >= buffer.options.Capacity {
		return nil, ErrAsyncInsertBufferFull
	}
	if buffer.current == nil {
		buffer.current = &asyncInsertBatch{
			done:     make(chan struct{}),
			requests: make([]CacheCommandRequest, 0, buffer.options.BatchSize),
		}
	}
	batch := buffer.current
	index := len(batch.requests)
	batch.requests = append(batch.requests, request)
	buffer.queued++
	if len(batch.requests) >= buffer.options.BatchSize {
		buffer.ready = append(buffer.ready, batch)
		buffer.current = nil
		buffer.signalLocked()
	}
	return &AsyncInsertSubmission{batch: batch, index: index}, nil
}

func validateAsyncInsertRequest(request CacheCommandRequest) error {
	command := strings.ToUpper(strings.TrimSpace(request.Command))
	if command == "" || command == "BATCH" || request.IdempotencyKey != "" || !commandShouldJournal(request) {
		return ErrAsyncInsertBufferWriteOnly
	}
	return nil
}

func (buffer *AsyncInsertBuffer) signalLocked() {
	select {
	case buffer.wake <- struct{}{}:
	default:
	}
}

func (buffer *AsyncInsertBuffer) detachCurrentLocked() {
	if buffer.current == nil || len(buffer.current.requests) == 0 {
		return
	}
	buffer.ready = append(buffer.ready, buffer.current)
	buffer.current = nil
}

func (buffer *AsyncInsertBuffer) takeReady() *asyncInsertBatch {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	if len(buffer.ready) == 0 {
		return nil
	}
	batch := buffer.ready[0]
	copy(buffer.ready, buffer.ready[1:])
	buffer.ready[len(buffer.ready)-1] = nil
	buffer.ready = buffer.ready[:len(buffer.ready)-1]
	buffer.queued -= len(batch.requests)
	return batch
}

func (buffer *AsyncInsertBuffer) flushReady() error {
	var firstErr error
	for {
		batch := buffer.takeReady()
		if batch == nil {
			return firstErr
		}
		if err := buffer.flushBatch(batch); err != nil && firstErr == nil {
			firstErr = err
		}
	}
}

func (buffer *AsyncInsertBuffer) flushAll() error {
	buffer.mu.Lock()
	buffer.detachCurrentLocked()
	buffer.mu.Unlock()
	return buffer.flushReady()
}

func (buffer *AsyncInsertBuffer) flushBatch(batch *asyncInsertBatch) error {
	if batch == nil || len(batch.requests) == 0 {
		return nil
	}
	submissions := make([]*CommandJournalSubmission, len(batch.requests))
	for index, request := range batch.requests {
		for {
			submission, err := buffer.journal.SubmitAsyncCommand(buffer.trie, request)
			if err == nil {
				submissions[index] = submission
				break
			}
			if !errors.Is(err, ErrCommandJournalAsyncQueueFull) {
				batch.complete(CacheCommandResponse{}, err)
				return err
			}
			time.Sleep(time.Millisecond)
		}
	}

	responses := make([]CacheCommandResponse, len(submissions))
	for index, submission := range submissions {
		response, err := submission.Wait(context.Background())
		if err != nil {
			batch.complete(CacheCommandResponse{}, err)
			return err
		}
		responses[index] = response
	}
	batch.complete(CacheCommandResponse{
		OK:        true,
		Message:   "batch completed",
		Responses: responses,
	}, nil)
	return nil
}

func (batch *asyncInsertBatch) complete(response CacheCommandResponse, err error) {
	batch.mu.Lock()
	defer batch.mu.Unlock()
	if batch.completed {
		return
	}
	batch.response = cloneCacheCommandResponse(response)
	batch.err = err
	batch.requestCount = len(batch.requests)
	batch.requests = nil
	batch.completed = true
	close(batch.done)
}

// Done returns a channel closed after the containing batch is durable and
// applied. A nil receiver returns nil.
func (submission *AsyncInsertSubmission) Done() <-chan struct{} {
	if submission == nil || submission.batch == nil {
		return nil
	}
	return submission.batch.done
}

// Status reports whether the containing batch has completed.
func (submission *AsyncInsertSubmission) Status() AsyncCommandSubmissionStatus {
	if submission == nil || submission.batch == nil {
		return AsyncCommandSubmissionUnknown
	}
	select {
	case <-submission.batch.done:
		return AsyncCommandSubmissionCompleted
	default:
		return AsyncCommandSubmissionPending
	}
}

// Wait waits for durable-and-applied completion and returns the response for
// this command. Context cancellation only stops waiting; it does not cancel
// the accepted write.
func (submission *AsyncInsertSubmission) Wait(ctx context.Context) (CacheCommandResponse, error) {
	if submission == nil || submission.batch == nil {
		return CacheCommandResponse{}, ErrNilAsyncInsertSubmission
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-submission.batch.done:
	case <-ctx.Done():
		return CacheCommandResponse{}, ctx.Err()
	}
	submission.batch.mu.Lock()
	response := cloneCacheCommandResponse(submission.batch.response)
	err := submission.batch.err
	requestCount := submission.batch.requestCount
	submission.batch.mu.Unlock()
	if err != nil {
		return CacheCommandResponse{}, err
	}
	if submission.index < len(response.Responses) {
		return response.Responses[submission.index], nil
	}
	if requestCount == 1 {
		return response, nil
	}
	return CacheCommandResponse{}, fmt.Errorf("hatriecache: async insert batch response count %d, want at least %d", len(response.Responses), submission.index+1)
}

// Flush synchronously drains all currently admitted commands. Commands
// submitted after Flush begins may be included if they win the same worker
// handoff; no command is canceled by a caller timeout.
func (buffer *AsyncInsertBuffer) Flush(ctx context.Context) error {
	if buffer == nil {
		return ErrAsyncInsertBufferClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ack := make(chan error, 1)
	buffer.mu.Lock()
	if buffer.closed {
		err := buffer.closeErr
		buffer.mu.Unlock()
		if err != nil {
			return err
		}
		return ErrAsyncInsertBufferClosed
	}
	buffer.mu.Unlock()
	select {
	case buffer.flushRequest <- ack:
	case <-buffer.done:
		return buffer.closeError()
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-ack:
		return err
	case <-buffer.done:
		return buffer.closeError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes admitted writes and stops the background worker. It is
// idempotent; a timeout leaves the worker running so a later Close can wait
// for the same drain to finish.
func (buffer *AsyncInsertBuffer) Close(ctx context.Context) error {
	if buffer == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	buffer.closeOnce.Do(func() {
		buffer.mu.Lock()
		buffer.closed = true
		close(buffer.close)
		buffer.mu.Unlock()
	})
	select {
	case <-buffer.done:
		return buffer.closeError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (buffer *AsyncInsertBuffer) closeError() error {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.closeErr
}

func (buffer *AsyncInsertBuffer) run() {
	ticker := time.NewTicker(buffer.options.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-buffer.wake:
			_ = buffer.flushReady()
		case <-ticker.C:
			_ = buffer.flushAll()
		case ack := <-buffer.flushRequest:
			ack <- buffer.flushAll()
		case <-buffer.close:
			err := buffer.flushAll()
			buffer.mu.Lock()
			buffer.closeErr = err
			buffer.mu.Unlock()
			close(buffer.done)
			return
		}
	}
}
