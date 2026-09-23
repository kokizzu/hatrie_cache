package hatSql

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	defaultMutationQueueCapacity   = 64
	defaultMutationWorkers         = 1
	defaultMutationHistoryCapacity = 128
	maxMutationQueueCapacity       = 4096
	maxMutationWorkers             = 64
	maxMutationHistoryCapacity     = 4096
	maxMutationIDLength            = 256
	maxMutationErrorLength         = 4096
)

var (
	ErrMutationControllerNil    = errors.New("mutation controller is nil")
	ErrMutationControllerClosed = errors.New("mutation controller is closed")
	ErrMutationIDRequired       = errors.New("mutation id is required")
	ErrMutationIDTooLong        = errors.New("mutation id is too long")
	ErrMutationRunRequired      = errors.New("mutation run function is required")
	ErrMutationQueueFull        = errors.New("mutation queue is full")
	ErrMutationAlreadyExists    = errors.New("mutation id already exists")
	ErrMutationNotFound         = errors.New("mutation not found")
	ErrMutationNotRunning       = errors.New("mutation is not running")
	ErrMutationProgressInvalid  = errors.New("mutation progress is invalid")
	ErrMutationOptionInvalid    = errors.New("mutation controller option is invalid")
)

// MutationState describes the lifecycle state of a submitted mutation.
type MutationState string

const (
	MutationQueued    MutationState = "queued"
	MutationRunning   MutationState = "running"
	MutationSucceeded MutationState = "succeeded"
	MutationFailed    MutationState = "failed"
	MutationCanceled  MutationState = "canceled"

	// MutationCancelled is retained as a spelling alias for callers using British English.
	MutationCancelled = MutationCanceled
)

// MutationProgress is the latest progress reported by a running mutation.
type MutationProgress struct {
	Completed uint64
	Total     uint64
}

// MutationProgressReporter records monotonic progress for a running mutation.
type MutationProgressReporter func(MutationProgress) error

// MutationSpec describes one queued mutation.
type MutationSpec struct {
	ID       string
	Priority int
	Run      func(context.Context, MutationProgressReporter) error
}

// MutationControllerOptions controls queue, worker, and terminal history bounds.
// Zero values use conservative defaults.
type MutationControllerOptions struct {
	QueueCapacity   int
	Workers         int
	HistoryCapacity int
}

// MutationSnapshot is a point-in-time, copy-safe mutation status record.
type MutationSnapshot struct {
	ID         string
	Priority   int
	State      MutationState
	Completed  uint64
	Total      uint64
	Error      string
	StartedAt  time.Time
	FinishedAt time.Time
}

// MutationControllerStats contains bounded-controller counters and gauges.
type MutationControllerStats struct {
	QueueCapacity int
	Workers       int
	Queued        int
	Running       int
	Closed        bool
	Submitted     uint64
	Completed     uint64
	Failed        uint64
	Canceled      uint64
	Rejected      uint64
}

// MutationHandle controls and observes one submitted mutation.
type MutationHandle struct {
	controller *MutationController
	job        *mutationJob
}

// ID returns the stable mutation identifier.
func (h MutationHandle) ID() string {
	if h.job == nil {
		return ""
	}
	return h.job.spec.ID
}

// Done returns a channel closed when the mutation reaches a terminal state.
func (h MutationHandle) Done() <-chan struct{} {
	if h.job == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return h.job.done
}

// Wait waits for the mutation and returns its terminal error.
func (h MutationHandle) Wait(ctx context.Context) error {
	if h.job == nil {
		return ErrMutationNotFound
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-h.job.done:
		return h.job.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cancel requests cancellation of the mutation.
func (h MutationHandle) Cancel() error {
	if h.controller == nil {
		return ErrMutationControllerNil
	}
	return h.controller.Cancel(h.ID())
}

// Snapshot returns the latest state for the mutation.
func (h MutationHandle) Snapshot() (MutationSnapshot, error) {
	if h.controller == nil {
		return MutationSnapshot{}, ErrMutationControllerNil
	}
	return h.controller.Snapshot(h.ID())
}

type mutationJob struct {
	spec            MutationSpec
	sequence        uint64
	ctx             context.Context
	cancel          context.CancelFunc
	done            chan struct{}
	state           MutationState
	progress        MutationProgress
	totalKnown      bool
	err             error
	startedAt       time.Time
	finishedAt      time.Time
	cancelRequested bool
	index           int
}

type mutationHeap []*mutationJob

func (h mutationHeap) Len() int {
	return len(h)
}

func (h mutationHeap) Less(i, j int) bool {
	if h[i].spec.Priority != h[j].spec.Priority {
		return h[i].spec.Priority > h[j].spec.Priority
	}
	return h[i].sequence < h[j].sequence
}

func (h mutationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *mutationHeap) Push(value any) {
	job := value.(*mutationJob)
	job.index = len(*h)
	*h = append(*h, job)
}

func (h *mutationHeap) Pop() any {
	old := *h
	n := len(old)
	job := old[n-1]
	old[n-1] = nil
	job.index = -1
	*h = old[:n-1]
	return job
}

// MutationController executes explicitly submitted mutations with bounded admission.
// It is opt-in: constructing a controller does not change ordinary SQL execution.
type MutationController struct {
	mu              sync.Mutex
	cond            *sync.Cond
	queue           mutationHeap
	jobs            map[string]*mutationJob
	history         []MutationSnapshot
	queueCapacity   int
	workers         int
	historyCapacity int
	nextSequence    uint64
	queued          int
	running         int
	closed          bool
	submitted       uint64
	completed       uint64
	failed          uint64
	canceled        uint64
	rejected        uint64
	wg              sync.WaitGroup
}

// NewMutationController creates an opt-in bounded mutation executor.
func NewMutationController(options MutationControllerOptions) (*MutationController, error) {
	queueCapacity := options.QueueCapacity
	if queueCapacity <= 0 {
		queueCapacity = defaultMutationQueueCapacity
	}
	if queueCapacity > maxMutationQueueCapacity {
		return nil, fmt.Errorf("%w: queue capacity %d exceeds %d", ErrMutationOptionInvalid, queueCapacity, maxMutationQueueCapacity)
	}

	workers := options.Workers
	if workers <= 0 {
		workers = defaultMutationWorkers
	}
	if workers > maxMutationWorkers {
		return nil, fmt.Errorf("%w: workers %d exceeds %d", ErrMutationOptionInvalid, workers, maxMutationWorkers)
	}

	historyCapacity := options.HistoryCapacity
	if historyCapacity <= 0 {
		historyCapacity = defaultMutationHistoryCapacity
	}
	if historyCapacity > maxMutationHistoryCapacity {
		return nil, fmt.Errorf("%w: history capacity %d exceeds %d", ErrMutationOptionInvalid, historyCapacity, maxMutationHistoryCapacity)
	}

	controller := &MutationController{
		jobs:            make(map[string]*mutationJob),
		queueCapacity:   queueCapacity,
		workers:         workers,
		historyCapacity: historyCapacity,
	}
	controller.cond = sync.NewCond(&controller.mu)
	controller.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go controller.worker()
	}
	return controller, nil
}

// Submit admits a mutation if the bounded queue has capacity.
func (c *MutationController) Submit(ctx context.Context, spec MutationSpec) (MutationHandle, error) {
	if c == nil {
		return MutationHandle{}, ErrMutationControllerNil
	}

	id := strings.TrimSpace(spec.ID)
	if id == "" {
		return MutationHandle{}, ErrMutationIDRequired
	}
	if len(id) > maxMutationIDLength {
		return MutationHandle{}, ErrMutationIDTooLong
	}
	if spec.Run == nil {
		return MutationHandle{}, ErrMutationRunRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return MutationHandle{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return MutationHandle{}, ErrMutationControllerClosed
	}
	if err := ctx.Err(); err != nil {
		return MutationHandle{}, err
	}
	if _, exists := c.jobs[id]; exists || c.historyContainsLocked(id) {
		return MutationHandle{}, ErrMutationAlreadyExists
	}
	if c.queued >= c.queueCapacity {
		c.rejected++
		return MutationHandle{}, ErrMutationQueueFull
	}

	jobContext, cancel := context.WithCancel(ctx)
	job := &mutationJob{
		spec:     MutationSpec{ID: id, Priority: spec.Priority, Run: spec.Run},
		sequence: c.nextSequence,
		ctx:      jobContext,
		cancel:   cancel,
		done:     make(chan struct{}),
		state:    MutationQueued,
		index:    -1,
	}
	c.nextSequence++
	c.jobs[id] = job
	heap.Push(&c.queue, job)
	c.queued++
	c.submitted++
	c.cond.Signal()
	return MutationHandle{controller: c, job: job}, nil
}

// Cancel requests cancellation of a queued or running mutation.
func (c *MutationController) Cancel(id string) error {
	if c == nil {
		return ErrMutationControllerNil
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrMutationIDRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	job, ok := c.jobs[id]
	if !ok {
		return ErrMutationNotFound
	}
	job.cancelRequested = true
	job.cancel()
	if job.state == MutationQueued {
		heap.Remove(&c.queue, job.index)
		c.queued--
		c.finishLocked(job, MutationCanceled, context.Canceled)
	}
	return nil
}

// Snapshot returns the latest active or retained terminal state for an ID.
func (c *MutationController) Snapshot(id string) (MutationSnapshot, error) {
	if c == nil {
		return MutationSnapshot{}, ErrMutationControllerNil
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return MutationSnapshot{}, ErrMutationIDRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if job, ok := c.jobs[id]; ok {
		return c.snapshotLocked(job), nil
	}
	for i := len(c.history) - 1; i >= 0; i-- {
		if c.history[i].ID == id {
			return c.history[i], nil
		}
	}
	return MutationSnapshot{}, ErrMutationNotFound
}

// Snapshots returns retained terminal snapshots from oldest to newest.
func (c *MutationController) Snapshots() []MutationSnapshot {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]MutationSnapshot(nil), c.history...)
}

// Stats returns current bounded-controller gauges and counters.
func (c *MutationController) Stats() MutationControllerStats {
	if c == nil {
		return MutationControllerStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return MutationControllerStats{
		QueueCapacity: c.queueCapacity,
		Workers:       c.workers,
		Queued:        c.queued,
		Running:       c.running,
		Closed:        c.closed,
		Submitted:     c.submitted,
		Completed:     c.completed,
		Failed:        c.failed,
		Canceled:      c.canceled,
		Rejected:      c.rejected,
	}
}

// Close cancels queued/running work and waits for workers to exit.
// A running callback must honor its context for Close to return promptly.
func (c *MutationController) Close() error {
	if c == nil {
		return ErrMutationControllerNil
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	for len(c.queue) > 0 {
		job := heap.Pop(&c.queue).(*mutationJob)
		c.queued--
		job.cancelRequested = true
		job.cancel()
		c.finishLocked(job, MutationCanceled, context.Canceled)
	}
	for _, job := range c.jobs {
		if job.state == MutationRunning {
			job.cancelRequested = true
			job.cancel()
		}
	}
	c.cond.Broadcast()
	c.mu.Unlock()

	c.wg.Wait()
	return nil
}

func (c *MutationController) worker() {
	defer c.wg.Done()
	for {
		job := c.nextJob()
		if job == nil {
			return
		}

		err := c.run(job)

		c.mu.Lock()
		c.running--
		state := MutationSucceeded
		if job.cancelRequested || job.ctx.Err() != nil {
			state = MutationCanceled
			if err == nil {
				err = job.ctx.Err()
			}
			if err == nil {
				err = context.Canceled
			}
		} else if err != nil {
			state = MutationFailed
		}
		c.finishLocked(job, state, err)
		c.mu.Unlock()
	}
}

func (c *MutationController) nextJob() *mutationJob {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.queue) == 0 && !c.closed {
		c.cond.Wait()
	}
	if len(c.queue) == 0 {
		return nil
	}
	job := heap.Pop(&c.queue).(*mutationJob)
	c.queued--
	job.state = MutationRunning
	job.startedAt = time.Now().UTC()
	c.running++
	return job
}

func (c *MutationController) run(job *mutationJob) (err error) {
	if ctxErr := job.ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("mutation %q panicked: %v", job.spec.ID, recovered)
		}
	}()
	return job.spec.Run(job.ctx, func(progress MutationProgress) error {
		return c.report(job, progress)
	})
}

func (c *MutationController) report(job *mutationJob, progress MutationProgress) error {
	if err := job.ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := job.ctx.Err(); err != nil {
		return err
	}
	if job.state != MutationRunning {
		return ErrMutationNotRunning
	}
	if progress.Completed < job.progress.Completed {
		return fmt.Errorf("%w: completed progress regressed", ErrMutationProgressInvalid)
	}
	if progress.Total != 0 {
		if job.totalKnown && progress.Total != job.progress.Total {
			return fmt.Errorf("%w: total changed", ErrMutationProgressInvalid)
		}
		if progress.Completed > progress.Total {
			return fmt.Errorf("%w: completed exceeds total", ErrMutationProgressInvalid)
		}
		job.totalKnown = true
	} else if job.totalKnown && progress.Completed > job.progress.Total {
		return fmt.Errorf("%w: completed exceeds total", ErrMutationProgressInvalid)
	}
	if progress.Total == 0 && job.totalKnown {
		progress.Total = job.progress.Total
	}
	job.progress = progress
	return nil
}

func (c *MutationController) finishLocked(job *mutationJob, state MutationState, err error) {
	if _, exists := c.jobs[job.spec.ID]; !exists {
		return
	}
	job.state = state
	job.err = err
	job.finishedAt = time.Now().UTC()
	delete(c.jobs, job.spec.ID)
	c.history = append(c.history, c.snapshotLocked(job))
	if len(c.history) > c.historyCapacity {
		copy(c.history, c.history[len(c.history)-c.historyCapacity:])
		c.history = c.history[:c.historyCapacity]
	}
	switch state {
	case MutationSucceeded:
		c.completed++
	case MutationFailed:
		c.failed++
	case MutationCanceled:
		c.canceled++
	}
	close(job.done)
}

func (c *MutationController) snapshotLocked(job *mutationJob) MutationSnapshot {
	return MutationSnapshot{
		ID:         job.spec.ID,
		Priority:   job.spec.Priority,
		State:      job.state,
		Completed:  job.progress.Completed,
		Total:      job.progress.Total,
		Error:      boundedMutationError(job.err),
		StartedAt:  job.startedAt,
		FinishedAt: job.finishedAt,
	}
}

func (c *MutationController) historyContainsLocked(id string) bool {
	for _, snapshot := range c.history {
		if snapshot.ID == id {
			return true
		}
	}
	return false
}

func boundedMutationError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) <= maxMutationErrorLength {
		return message
	}
	return message[:maxMutationErrorLength]
}
