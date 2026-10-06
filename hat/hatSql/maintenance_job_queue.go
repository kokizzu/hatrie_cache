package hatSql

import (
	"context"
	"strings"
	"time"
)

// SQLMaintenanceJobKind identifies the maintenance operation being queued.
// The callback remains caller-owned so the queue can be used by table,
// projection, index, and storage integrations without coupling those systems.
type SQLMaintenanceJobKind string

const (
	SQLMaintenanceJobOptimize     SQLMaintenanceJobKind = "optimize"
	SQLMaintenanceJobMerge        SQLMaintenanceJobKind = "merge"
	SQLMaintenanceJobIndexRebuild SQLMaintenanceJobKind = "index_rebuild"
	SQLMaintenanceJobMutation     SQLMaintenanceJobKind = "mutation"
	SQLMaintenanceJobCustom       SQLMaintenanceJobKind = "custom"
)

// SQLMaintenanceJobState is shared with the existing index rebuild queue so
// callers see identical lifecycle semantics across specialized and generic
// maintenance APIs.
type SQLMaintenanceJobState = SQLIndexRebuildState

const (
	SQLMaintenanceJobQueued    SQLMaintenanceJobState = SQLIndexRebuildQueued
	SQLMaintenanceJobRunning   SQLMaintenanceJobState = SQLIndexRebuildRunning
	SQLMaintenanceJobSucceeded SQLMaintenanceJobState = SQLIndexRebuildSucceeded
	SQLMaintenanceJobFailed    SQLMaintenanceJobState = SQLIndexRebuildFailed
	SQLMaintenanceJobCanceled  SQLMaintenanceJobState = SQLIndexRebuildCanceled
)

// SQLMaintenanceProgressFunc reports monotone callback progress.
type SQLMaintenanceProgressFunc = SQLIndexRebuildProgressFunc

// SQLMaintenanceRunFunc executes one maintenance task. It must honor ctx and
// must not publish partial state when returning an error.
type SQLMaintenanceRunFunc = SQLIndexRebuildFunc

// SQLMaintenanceVerifyFunc validates a task's result before success is
// published. A non-nil error leaves the task failed.
type SQLMaintenanceVerifyFunc = SQLIndexRebuildVerifyFunc

// SQLMaintenanceQueueOptions bounds generic maintenance state. Workers zero
// keeps the queue disabled until an application explicitly opts in.
type SQLMaintenanceQueueOptions struct {
	Capacity        int
	Workers         int
	HistoryCapacity int
}

// SQLMaintenanceJobRequest describes one optimize, merge, rebuild, mutation,
// or custom task.
type SQLMaintenanceJobRequest struct {
	ID       string
	Name     string
	Kind     SQLMaintenanceJobKind
	Priority int
	Run      SQLMaintenanceRunFunc
	Verify   SQLMaintenanceVerifyFunc
}

// SQLMaintenanceJobStatus is an immutable snapshot of one queued task.
type SQLMaintenanceJobStatus struct {
	ID                    string
	Name                  string
	Kind                  SQLMaintenanceJobKind
	Priority              int
	State                 SQLMaintenanceJobState
	SubmittedAt           time.Time
	StartedAt             time.Time
	FinishedAt            time.Time
	Completed             int
	Total                 int
	CancelRequested       bool
	VerificationRequested bool
	Verified              bool
	Error                 string
}

var (
	ErrSQLMaintenanceQueueNil       = ErrSQLIndexRebuildQueueNil
	ErrSQLMaintenanceQueueClosed    = ErrSQLIndexRebuildQueueClosed
	ErrSQLMaintenanceQueueStarted   = ErrSQLIndexRebuildQueueStarted
	ErrSQLMaintenanceQueueDisabled  = ErrSQLIndexRebuildQueueDisabled
	ErrSQLMaintenanceQueueFull      = ErrSQLIndexRebuildQueueFull
	ErrSQLMaintenanceJobExists      = ErrSQLIndexRebuildTaskExists
	ErrSQLMaintenanceJobNotFound    = ErrSQLIndexRebuildTaskNotFound
	ErrSQLMaintenanceRequestInvalid = ErrSQLIndexRebuildRequestInvalid
)

// SQLMaintenanceQueue provides one bounded priority queue for maintenance
// callbacks. It reuses SQLIndexRebuildQueue's cancellation, worker, history,
// and idle-wait implementation while retaining the job kind at this layer.
type SQLMaintenanceQueue struct {
	queue *SQLIndexRebuildQueue
}

// NewSQLMaintenanceQueue creates a generic maintenance queue. A zero Workers
// value is intentionally disabled by default; enqueueing remains useful for
// inspection, but Start and Flush return ErrSQLMaintenanceQueueDisabled.
func NewSQLMaintenanceQueue(options SQLMaintenanceQueueOptions) (*SQLMaintenanceQueue, error) {
	queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{
		Capacity:        options.Capacity,
		Workers:         options.Workers,
		HistoryCapacity: options.HistoryCapacity,
	})
	if err != nil {
		return nil, err
	}
	return &SQLMaintenanceQueue{queue: queue}, nil
}

// Start starts the configured worker pool. It is safe to call only once.
func (queue *SQLMaintenanceQueue) Start(ctx context.Context) error {
	if queue == nil {
		return ErrSQLMaintenanceQueueNil
	}
	return queue.queue.Start(ctx)
}

// Enqueue adds one bounded maintenance task. Priority is descending, and
// equal-priority tasks retain FIFO submission order.
func (queue *SQLMaintenanceQueue) Enqueue(request SQLMaintenanceJobRequest) (SQLMaintenanceJobStatus, error) {
	if queue == nil {
		return SQLMaintenanceJobStatus{}, ErrSQLMaintenanceQueueNil
	}
	request.ID = strings.TrimSpace(request.ID)
	request.Name = strings.TrimSpace(request.Name)
	request.Kind = SQLMaintenanceJobKind(strings.TrimSpace(string(request.Kind)))
	if request.ID == "" || request.Name == "" || request.Kind == "" || request.Run == nil || strings.IndexByte(request.Name, 0) >= 0 || strings.IndexByte(string(request.Kind), 0) >= 0 {
		return SQLMaintenanceJobStatus{}, ErrSQLMaintenanceRequestInvalid
	}
	underlying, err := queue.queue.Enqueue(SQLIndexRebuildRequest{
		ID:       request.ID,
		Name:     encodeMaintenanceJobName(request.Name, request.Kind),
		Priority: request.Priority,
		Run:      request.Run,
		Verify:   request.Verify,
	})
	if err != nil {
		return SQLMaintenanceJobStatus{}, err
	}
	return queue.withKind(underlying), nil
}

// Cancel requests cancellation and returns the latest status. Running tasks
// become canceled only after their callback observes context cancellation.
func (queue *SQLMaintenanceQueue) Cancel(id string) (SQLMaintenanceJobStatus, error) {
	if queue == nil {
		return SQLMaintenanceJobStatus{}, ErrSQLMaintenanceQueueNil
	}
	status, err := queue.queue.Cancel(id)
	if err != nil {
		return SQLMaintenanceJobStatus{}, err
	}
	return queue.withKind(status), nil
}

// Status returns an active or retained history entry.
func (queue *SQLMaintenanceQueue) Status(id string) (SQLMaintenanceJobStatus, bool) {
	if queue == nil {
		return SQLMaintenanceJobStatus{}, false
	}
	status, ok := queue.queue.Status(id)
	if !ok {
		return SQLMaintenanceJobStatus{}, false
	}
	return queue.withKind(status), true
}

// Snapshot returns active jobs and retained history in submission order.
func (queue *SQLMaintenanceQueue) Snapshot() []SQLMaintenanceJobStatus {
	if queue == nil {
		return nil
	}
	underlying := queue.queue.Snapshot()
	statuses := make([]SQLMaintenanceJobStatus, len(underlying))
	for index, status := range underlying {
		statuses[index] = queue.withKind(status)
	}
	return statuses
}

// Flush waits until all active tasks finish or ctx is canceled.
func (queue *SQLMaintenanceQueue) Flush(ctx context.Context) error {
	if queue == nil {
		return ErrSQLMaintenanceQueueNil
	}
	return queue.queue.Flush(ctx)
}

// Close cancels workers and pending tasks, then waits for running callbacks.
func (queue *SQLMaintenanceQueue) Close() error {
	if queue == nil {
		return ErrSQLMaintenanceQueueNil
	}
	return queue.queue.Close()
}

func (queue *SQLMaintenanceQueue) withKind(status SQLIndexRebuildStatus) SQLMaintenanceJobStatus {
	kind, name := decodeMaintenanceJobName(status.Name)
	return SQLMaintenanceJobStatus{
		ID:                    status.ID,
		Name:                  name,
		Kind:                  kind,
		Priority:              status.Priority,
		State:                 status.State,
		SubmittedAt:           status.SubmittedAt,
		StartedAt:             status.StartedAt,
		FinishedAt:            status.FinishedAt,
		Completed:             status.Completed,
		Total:                 status.Total,
		CancelRequested:       status.CancelRequested,
		VerificationRequested: status.VerificationRequested,
		Verified:              status.Verified,
		Error:                 status.Error,
	}
}

const maintenanceJobNameSeparator byte = 0

func encodeMaintenanceJobName(name string, kind SQLMaintenanceJobKind) string {
	return string(kind) + string([]byte{maintenanceJobNameSeparator}) + name
}

func decodeMaintenanceJobName(encoded string) (SQLMaintenanceJobKind, string) {
	separator := strings.IndexByte(encoded, maintenanceJobNameSeparator)
	if separator <= 0 {
		return "", encoded
	}
	return SQLMaintenanceJobKind(encoded[:separator]), encoded[separator+1:]
}
