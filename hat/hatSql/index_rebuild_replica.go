package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrSQLIndexRebuildReplicaSetInvalid        = errors.New("sql index rebuild replica set options are invalid")
	ErrSQLIndexRebuildReplicaSetClosed         = errors.New("sql index rebuild replica set is closed")
	ErrSQLIndexRebuildReplicaTaskNotFound      = errors.New("sql index rebuild replica task was not found")
	ErrSQLIndexRebuildReplicaQuorumUnavailable = errors.New("sql index rebuild replica quorum is unavailable")
)

// SQLIndexRebuildReplicaSetOptions configures replicated execution over
// independent bounded rebuild queues. Queues are expected to belong to
// independent workers or processes when the caller needs failure isolation.
// A zero quorum requires one successful replica, while all accepted replicas
// continue running to converge their maintained state.
type SQLIndexRebuildReplicaSetOptions struct {
	Queues []*SQLIndexRebuildQueue
	Quorum int
}

// SQLIndexRebuildReplicaStatus is the quorum view of one replicated rebuild.
// State becomes succeeded as soon as Quorum replicas succeed; remaining
// replicas continue to run and are visible in ReplicaStatuses.
type SQLIndexRebuildReplicaStatus struct {
	ID              string                  `json:"id"`
	State           SQLIndexRebuildState    `json:"state"`
	Quorum          int                     `json:"quorum"`
	Successes       int                     `json:"successes"`
	Terminal        int                     `json:"terminal"`
	ReplicaStatuses []SQLIndexRebuildStatus `json:"replica_statuses"`
}

type SQLIndexRebuildReplicaSet struct {
	mu         sync.Mutex
	queues     []*SQLIndexRebuildQueue
	quorum     int
	operations map[string]*sqlIndexRebuildReplicaOperation
	active     map[*SQLIndexRebuildQueue]bool
	started    bool
	closed     bool
}

type sqlIndexRebuildReplicaOperation struct {
	id       string
	quorum   int
	ready    bool
	replicas []sqlIndexRebuildReplicaTask
}

type sqlIndexRebuildReplicaTask struct {
	queue  *SQLIndexRebuildQueue
	status SQLIndexRebuildStatus
}

// NewSQLIndexRebuildReplicaSet creates an opt-in replicated rebuild
// coordinator. It does not start its queues automatically.
func NewSQLIndexRebuildReplicaSet(options SQLIndexRebuildReplicaSetOptions) (*SQLIndexRebuildReplicaSet, error) {
	if len(options.Queues) == 0 || len(options.Queues) > MaxSQLIndexRebuildQueueWorkers {
		return nil, ErrSQLIndexRebuildReplicaSetInvalid
	}
	quorum := options.Quorum
	if quorum == 0 {
		quorum = 1
	}
	if quorum < 1 || quorum > len(options.Queues) {
		return nil, ErrSQLIndexRebuildReplicaSetInvalid
	}
	seen := make(map[*SQLIndexRebuildQueue]struct{}, len(options.Queues))
	queues := make([]*SQLIndexRebuildQueue, len(options.Queues))
	for index, queue := range options.Queues {
		if queue == nil {
			return nil, ErrSQLIndexRebuildReplicaSetInvalid
		}
		if _, exists := seen[queue]; exists {
			return nil, ErrSQLIndexRebuildReplicaSetInvalid
		}
		seen[queue] = struct{}{}
		queues[index] = queue
	}
	return &SQLIndexRebuildReplicaSet{
		queues:     queues,
		quorum:     quorum,
		operations: make(map[string]*sqlIndexRebuildReplicaOperation),
		active:     make(map[*SQLIndexRebuildQueue]bool, len(queues)),
	}, nil
}

// Start starts every replica queue. A failed minority does not prevent start
// when enough queues are already running to satisfy the configured quorum.
func (set *SQLIndexRebuildReplicaSet) Start(ctx context.Context) error {
	if set == nil {
		return ErrSQLIndexRebuildReplicaSetInvalid
	}
	if ctx == nil {
		return ErrSQLIndexRebuildReplicaSetInvalid
	}
	set.mu.Lock()
	if set.closed {
		set.mu.Unlock()
		return ErrSQLIndexRebuildReplicaSetClosed
	}
	queues := append([]*SQLIndexRebuildQueue(nil), set.queues...)
	quorum := set.quorum
	set.mu.Unlock()

	started := 0
	var firstErr error
	startedQueues := make(map[*SQLIndexRebuildQueue]bool, len(queues))
	for _, queue := range queues {
		err := queue.Start(ctx)
		if err == nil || errors.Is(err, ErrSQLIndexRebuildQueueStarted) {
			started++
			startedQueues[queue] = true
			continue
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	set.mu.Lock()
	set.started = true
	for _, queue := range queues {
		if startedQueues[queue] {
			set.active[queue] = true
		}
	}
	set.mu.Unlock()
	if started < quorum {
		if firstErr == nil {
			firstErr = ErrSQLIndexRebuildReplicaQuorumUnavailable
		}
		return fmt.Errorf("%w: started %d of %d queues: %v", ErrSQLIndexRebuildReplicaQuorumUnavailable, started, quorum, firstErr)
	}
	return nil
}

// Enqueue fans one idempotent, revision-fenced rebuild request to every
// replica queue. Run and Verify must tolerate duplicate execution because
// more than one worker can complete the same logical operation.
func (set *SQLIndexRebuildReplicaSet) Enqueue(request SQLIndexRebuildRequest) (SQLIndexRebuildReplicaStatus, error) {
	if set == nil {
		return SQLIndexRebuildReplicaStatus{}, ErrSQLIndexRebuildReplicaSetInvalid
	}
	request.ID = strings.TrimSpace(request.ID)
	request.Name = strings.TrimSpace(request.Name)
	if request.ID == "" || request.Name == "" || request.Run == nil {
		return SQLIndexRebuildReplicaStatus{}, ErrSQLIndexRebuildRequestInvalid
	}

	set.mu.Lock()
	if set.closed {
		set.mu.Unlock()
		return SQLIndexRebuildReplicaStatus{}, ErrSQLIndexRebuildReplicaSetClosed
	}
	if _, exists := set.operations[request.ID]; exists {
		set.mu.Unlock()
		return SQLIndexRebuildReplicaStatus{}, ErrSQLIndexRebuildTaskExists
	}
	operation := &sqlIndexRebuildReplicaOperation{id: request.ID, quorum: set.quorum}
	set.operations[request.ID] = operation
	queues := append([]*SQLIndexRebuildQueue(nil), set.queues...)
	started := set.started
	active := make(map[*SQLIndexRebuildQueue]bool, len(set.active))
	for queue, isActive := range set.active {
		active[queue] = isActive
	}
	set.mu.Unlock()

	tasks := make([]sqlIndexRebuildReplicaTask, 0, len(queues))
	for index, queue := range queues {
		replicaRequest := request
		replicaRequest.ID = sqlIndexRebuildReplicaTaskID(request.ID, index)
		if started && !active[queue] {
			tasks = append(tasks, sqlIndexRebuildReplicaTask{status: SQLIndexRebuildStatus{
				ID:    replicaRequest.ID,
				Name:  request.Name,
				State: SQLIndexRebuildFailed,
				Error: ErrSQLIndexRebuildReplicaQuorumUnavailable.Error(),
			}})
			continue
		}
		status, err := queue.Enqueue(replicaRequest)
		if err != nil {
			status = SQLIndexRebuildStatus{
				ID:    replicaRequest.ID,
				Name:  request.Name,
				State: SQLIndexRebuildFailed,
				Error: err.Error(),
			}
			tasks = append(tasks, sqlIndexRebuildReplicaTask{status: status})
			continue
		}
		tasks = append(tasks, sqlIndexRebuildReplicaTask{queue: queue, status: status})
	}

	set.mu.Lock()
	operation.replicas = tasks
	operation.ready = true
	set.mu.Unlock()
	result := set.aggregateOperation(operation)
	if result.Successes+len(queues)-result.Terminal < result.Quorum {
		set.cancelOperation(operation)
		return set.aggregateOperation(operation), ErrSQLIndexRebuildReplicaQuorumUnavailable
	}
	return result, nil
}

// Status returns the latest quorum and per-replica states for one operation.
func (set *SQLIndexRebuildReplicaSet) Status(id string) (SQLIndexRebuildReplicaStatus, bool) {
	if set == nil {
		return SQLIndexRebuildReplicaStatus{}, false
	}
	id = strings.TrimSpace(id)
	set.mu.Lock()
	operation, exists := set.operations[id]
	if !exists {
		set.mu.Unlock()
		return SQLIndexRebuildReplicaStatus{}, false
	}
	if !operation.ready {
		status := SQLIndexRebuildReplicaStatus{ID: id, State: SQLIndexRebuildQueued, Quorum: operation.quorum}
		set.mu.Unlock()
		return status, true
	}
	set.mu.Unlock()
	return set.aggregateOperation(operation), true
}

// Cancel requests cancellation on every accepted replica task.
func (set *SQLIndexRebuildReplicaSet) Cancel(id string) (SQLIndexRebuildReplicaStatus, error) {
	if set == nil {
		return SQLIndexRebuildReplicaStatus{}, ErrSQLIndexRebuildReplicaSetInvalid
	}
	set.mu.Lock()
	operation, exists := set.operations[strings.TrimSpace(id)]
	if !exists {
		set.mu.Unlock()
		return SQLIndexRebuildReplicaStatus{}, ErrSQLIndexRebuildReplicaTaskNotFound
	}
	tasks := append([]sqlIndexRebuildReplicaTask(nil), operation.replicas...)
	set.mu.Unlock()
	cancelSQLIndexRebuildReplicaTasks(tasks)
	return set.aggregateOperation(operation), nil
}

// Flush waits for all accepted replica queues to become idle.
func (set *SQLIndexRebuildReplicaSet) Flush(ctx context.Context) error {
	if set == nil {
		return ErrSQLIndexRebuildReplicaSetInvalid
	}
	if ctx == nil {
		return ErrSQLIndexRebuildReplicaSetInvalid
	}
	set.mu.Lock()
	if set.closed {
		set.mu.Unlock()
		return ErrSQLIndexRebuildReplicaSetClosed
	}
	queues := append([]*SQLIndexRebuildQueue(nil), set.queues...)
	if set.started {
		queues = queues[:0]
		for _, queue := range set.queues {
			if set.active[queue] {
				queues = append(queues, queue)
			}
		}
	}
	set.mu.Unlock()
	for _, queue := range queues {
		if err := queue.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Close closes every queue owned by the replica set. It is idempotent.
func (set *SQLIndexRebuildReplicaSet) Close() error {
	if set == nil {
		return ErrSQLIndexRebuildReplicaSetInvalid
	}
	set.mu.Lock()
	if set.closed {
		set.mu.Unlock()
		return nil
	}
	set.closed = true
	queues := append([]*SQLIndexRebuildQueue(nil), set.queues...)
	set.mu.Unlock()
	var firstErr error
	for _, queue := range queues {
		if err := queue.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (set *SQLIndexRebuildReplicaSet) aggregateOperation(operation *sqlIndexRebuildReplicaOperation) SQLIndexRebuildReplicaStatus {
	set.mu.Lock()
	tasks := append([]sqlIndexRebuildReplicaTask(nil), operation.replicas...)
	set.mu.Unlock()
	statuses := make([]SQLIndexRebuildStatus, len(tasks))
	for index, task := range tasks {
		statuses[index] = task.status
		if task.queue == nil {
			continue
		}
		if status, ok := task.queue.Status(task.status.ID); ok {
			statuses[index] = status
		} else {
			statuses[index].State = SQLIndexRebuildFailed
			statuses[index].Error = ErrSQLIndexRebuildTaskNotFound.Error()
		}
	}
	return aggregateSQLIndexRebuildReplicaStatuses(operation.id, operation.quorum, statuses)
}

func (set *SQLIndexRebuildReplicaSet) cancelOperation(operation *sqlIndexRebuildReplicaOperation) {
	set.mu.Lock()
	tasks := append([]sqlIndexRebuildReplicaTask(nil), operation.replicas...)
	set.mu.Unlock()
	cancelSQLIndexRebuildReplicaTasks(tasks)
}

func cancelSQLIndexRebuildReplicaTasks(tasks []sqlIndexRebuildReplicaTask) {
	for _, task := range tasks {
		if task.queue != nil {
			_, _ = task.queue.Cancel(task.status.ID)
		}
	}
}

func aggregateSQLIndexRebuildReplicaStatuses(id string, quorum int, statuses []SQLIndexRebuildStatus) SQLIndexRebuildReplicaStatus {
	result := SQLIndexRebuildReplicaStatus{
		ID:              id,
		State:           SQLIndexRebuildQueued,
		Quorum:          quorum,
		ReplicaStatuses: append([]SQLIndexRebuildStatus(nil), statuses...),
	}
	queued := false
	running := false
	for _, status := range statuses {
		switch status.State {
		case SQLIndexRebuildSucceeded:
			result.Successes++
			result.Terminal++
		case SQLIndexRebuildFailed, SQLIndexRebuildCanceled:
			result.Terminal++
		case SQLIndexRebuildRunning:
			running = true
		default:
			queued = true
		}
	}
	if result.Successes >= quorum {
		result.State = SQLIndexRebuildSucceeded
	} else if result.Successes+len(statuses)-result.Terminal < quorum {
		result.State = SQLIndexRebuildFailed
	} else if running {
		result.State = SQLIndexRebuildRunning
	} else if queued {
		result.State = SQLIndexRebuildQueued
	}
	return result
}

func sqlIndexRebuildReplicaTaskID(id string, replica int) string {
	return id + "#replica-" + strconv.Itoa(replica)
}
