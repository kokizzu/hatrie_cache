package hatSql

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultSQLIndexRebuildQueueCapacity        = 64
	DefaultSQLIndexRebuildQueueHistoryCapacity = 256
	MaxSQLIndexRebuildQueueCapacity            = 100_000
	MaxSQLIndexRebuildQueueWorkers             = 256
	MaxSQLIndexRebuildQueueHistoryCapacity     = 100_000
)

var (
	ErrSQLIndexRebuildQueueNil       = errors.New("sql index rebuild queue is nil")
	ErrSQLIndexRebuildQueueClosed    = errors.New("sql index rebuild queue is closed")
	ErrSQLIndexRebuildQueueStarted   = errors.New("sql index rebuild queue has already started")
	ErrSQLIndexRebuildQueueDisabled  = errors.New("sql index rebuild queue is disabled")
	ErrSQLIndexRebuildQueueFull      = errors.New("sql index rebuild queue is full")
	ErrSQLIndexRebuildTaskExists     = errors.New("sql index rebuild task already exists")
	ErrSQLIndexRebuildTaskNotFound   = errors.New("sql index rebuild task was not found")
	ErrSQLIndexRebuildRequestInvalid = errors.New("sql index rebuild request is invalid")
)

// SQLIndexRebuildQueueOptions controls the bounded background rebuild queue.
// Workers intentionally defaults to zero, which keeps the queue disabled until
// an application explicitly opts into background work.
type SQLIndexRebuildQueueOptions struct {
	Capacity        int
	Workers         int
	HistoryCapacity int
}

type SQLIndexRebuildState string

const (
	SQLIndexRebuildQueued    SQLIndexRebuildState = "queued"
	SQLIndexRebuildRunning   SQLIndexRebuildState = "running"
	SQLIndexRebuildSucceeded SQLIndexRebuildState = "succeeded"
	SQLIndexRebuildFailed    SQLIndexRebuildState = "failed"
	SQLIndexRebuildCanceled  SQLIndexRebuildState = "canceled"
)

type SQLIndexRebuildProgressFunc func(completed, total int)

type SQLIndexRebuildFunc func(context.Context, SQLIndexRebuildProgressFunc) error

type SQLIndexRebuildRequest struct {
	ID       string
	Name     string
	Priority int
	Run      SQLIndexRebuildFunc
}

type SQLIndexRebuildStatus struct {
	ID              string
	Name            string
	Priority        int
	State           SQLIndexRebuildState
	SubmittedAt     time.Time
	StartedAt       time.Time
	FinishedAt      time.Time
	Completed       int
	Total           int
	CancelRequested bool
	Error           string
}

type sqlIndexRebuildTask struct {
	request  SQLIndexRebuildRequest
	status   SQLIndexRebuildStatus
	sequence uint64
	heapPos  int
	ctx      context.Context
	cancel   context.CancelFunc
}

type sqlIndexRebuildHeap []*sqlIndexRebuildTask

func (tasks sqlIndexRebuildHeap) Len() int { return len(tasks) }

func (tasks sqlIndexRebuildHeap) Less(left, right int) bool {
	if tasks[left].request.Priority != tasks[right].request.Priority {
		return tasks[left].request.Priority > tasks[right].request.Priority
	}
	return tasks[left].sequence < tasks[right].sequence
}

func (tasks sqlIndexRebuildHeap) Swap(left, right int) {
	tasks[left], tasks[right] = tasks[right], tasks[left]
	tasks[left].heapPos = left
	tasks[right].heapPos = right
}

func (tasks *sqlIndexRebuildHeap) Push(value any) {
	task := value.(*sqlIndexRebuildTask)
	task.heapPos = len(*tasks)
	*tasks = append(*tasks, task)
}

func (tasks *sqlIndexRebuildHeap) Pop() any {
	old := *tasks
	last := len(old) - 1
	task := old[last]
	old[last] = nil
	task.heapPos = -1
	*tasks = old[:last]
	return task
}

type sqlIndexRebuildHistoryEntry struct {
	status   SQLIndexRebuildStatus
	sequence uint64
}

// SQLIndexRebuildQueue schedules explicitly submitted index rebuilds. It is
// useful for maintenance orchestration, not for making an individual rebuild
// faster: callers pay a small enqueue/scheduling cost in exchange for bounded
// concurrency, priorities, cancellation, and observable progress.
type SQLIndexRebuildQueue struct {
	mu      sync.Mutex
	options SQLIndexRebuildQueueOptions

	pending sqlIndexRebuildHeap
	tasks   map[string]*sqlIndexRebuildTask
	history map[string]sqlIndexRebuildHistoryEntry
	order   []string

	nextSequence uint64
	running      int
	wake         chan struct{}
	idle         chan struct{}
	idleClosed   bool

	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	closed  bool
	workers sync.WaitGroup
}

func NewSQLIndexRebuildQueue(options SQLIndexRebuildQueueOptions) (*SQLIndexRebuildQueue, error) {
	if options.Capacity < 0 || options.Workers < 0 || options.HistoryCapacity < 0 {
		return nil, ErrSQLIndexRebuildRequestInvalid
	}
	if options.Capacity == 0 {
		options.Capacity = DefaultSQLIndexRebuildQueueCapacity
	}
	if options.HistoryCapacity == 0 {
		options.HistoryCapacity = DefaultSQLIndexRebuildQueueHistoryCapacity
	}
	if options.Capacity > MaxSQLIndexRebuildQueueCapacity ||
		options.Workers > MaxSQLIndexRebuildQueueWorkers ||
		options.HistoryCapacity > MaxSQLIndexRebuildQueueHistoryCapacity {
		return nil, ErrSQLIndexRebuildRequestInvalid
	}
	idle := make(chan struct{})
	close(idle)
	wakeCapacity := options.Workers
	if wakeCapacity == 0 {
		wakeCapacity = 1
	}
	queue := &SQLIndexRebuildQueue{
		options:    options,
		tasks:      make(map[string]*sqlIndexRebuildTask),
		history:    make(map[string]sqlIndexRebuildHistoryEntry),
		wake:       make(chan struct{}, wakeCapacity),
		idle:       idle,
		idleClosed: true,
	}
	heap.Init(&queue.pending)
	return queue, nil
}

func (queue *SQLIndexRebuildQueue) Start(ctx context.Context) error {
	if queue == nil {
		return ErrSQLIndexRebuildQueueNil
	}
	if ctx == nil {
		return ErrSQLIndexRebuildRequestInvalid
	}
	queue.mu.Lock()
	if queue.closed {
		queue.mu.Unlock()
		return ErrSQLIndexRebuildQueueClosed
	}
	if queue.started {
		queue.mu.Unlock()
		return ErrSQLIndexRebuildQueueStarted
	}
	if queue.options.Workers == 0 {
		queue.mu.Unlock()
		return ErrSQLIndexRebuildQueueDisabled
	}
	queue.ctx, queue.cancel = context.WithCancel(ctx)
	queue.started = true
	queue.workers.Add(queue.options.Workers)
	workers := queue.options.Workers
	queue.mu.Unlock()
	for index := 0; index < workers; index++ {
		go queue.worker()
	}
	return nil
}

func (queue *SQLIndexRebuildQueue) Enqueue(request SQLIndexRebuildRequest) (SQLIndexRebuildStatus, error) {
	if queue == nil {
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildQueueNil
	}
	request.ID = strings.TrimSpace(request.ID)
	request.Name = strings.TrimSpace(request.Name)
	if request.ID == "" || request.Name == "" || request.Run == nil {
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildRequestInvalid
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.closed {
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildQueueClosed
	}
	if _, exists := queue.tasks[request.ID]; exists {
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildTaskExists
	}
	if _, exists := queue.history[request.ID]; exists {
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildTaskExists
	}
	if queue.pending.Len() >= queue.options.Capacity {
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildQueueFull
	}
	queue.nextSequence++
	task := &sqlIndexRebuildTask{
		request:  request,
		sequence: queue.nextSequence,
		status: SQLIndexRebuildStatus{
			ID:          request.ID,
			Name:        request.Name,
			Priority:    request.Priority,
			State:       SQLIndexRebuildQueued,
			SubmittedAt: time.Now(),
		},
	}
	wasIdle := queue.isIdleLocked()
	queue.tasks[request.ID] = task
	heap.Push(&queue.pending, task)
	queue.updateIdleLocked(wasIdle)
	queue.signalWakeLocked()
	return task.status, nil
}

func (queue *SQLIndexRebuildQueue) Cancel(id string) (SQLIndexRebuildStatus, error) {
	if queue == nil {
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildQueueNil
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildTaskNotFound
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	task, ok := queue.tasks[id]
	if !ok {
		if entry, exists := queue.history[id]; exists {
			return entry.status, nil
		}
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildTaskNotFound
	}
	if task.status.State == SQLIndexRebuildQueued {
		wasIdle := queue.isIdleLocked()
		heap.Remove(&queue.pending, task.heapPos)
		task.status.CancelRequested = true
		queue.finishLocked(task, SQLIndexRebuildCanceled, "")
		queue.updateIdleLocked(wasIdle)
		return task.status, nil
	}
	if task.status.State == SQLIndexRebuildRunning {
		task.status.CancelRequested = true
		if task.cancel != nil {
			task.cancel()
		}
	}
	return task.status, nil
}

func (queue *SQLIndexRebuildQueue) Status(id string) (SQLIndexRebuildStatus, bool) {
	if queue == nil {
		return SQLIndexRebuildStatus{}, false
	}
	id = strings.TrimSpace(id)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if task, ok := queue.tasks[id]; ok {
		return task.status, true
	}
	if entry, ok := queue.history[id]; ok {
		return entry.status, true
	}
	return SQLIndexRebuildStatus{}, false
}

func (queue *SQLIndexRebuildQueue) Snapshot() []SQLIndexRebuildStatus {
	if queue == nil {
		return nil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	type snapshotEntry struct {
		status   SQLIndexRebuildStatus
		sequence uint64
	}
	entries := make([]snapshotEntry, 0, len(queue.tasks)+len(queue.history))
	for _, task := range queue.tasks {
		entries = append(entries, snapshotEntry{status: task.status, sequence: task.sequence})
	}
	for _, entry := range queue.history {
		entries = append(entries, snapshotEntry{status: entry.status, sequence: entry.sequence})
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].sequence < entries[right].sequence
	})
	statuses := make([]SQLIndexRebuildStatus, len(entries))
	for index, entry := range entries {
		statuses[index] = entry.status
	}
	return statuses
}

func (queue *SQLIndexRebuildQueue) Flush(ctx context.Context) error {
	if queue == nil {
		return ErrSQLIndexRebuildQueueNil
	}
	if ctx == nil {
		return ErrSQLIndexRebuildRequestInvalid
	}
	queue.mu.Lock()
	if !queue.started && len(queue.tasks) > 0 {
		queue.mu.Unlock()
		return ErrSQLIndexRebuildQueueDisabled
	}
	if queue.isIdleLocked() {
		queue.mu.Unlock()
		return nil
	}
	idle := queue.idle
	queue.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (queue *SQLIndexRebuildQueue) Close() error {
	if queue == nil {
		return ErrSQLIndexRebuildQueueNil
	}
	queue.mu.Lock()
	if !queue.closed {
		queue.closed = true
		wasIdle := queue.isIdleLocked()
		if queue.cancel != nil {
			queue.cancel()
		}
		queue.cancelPendingLocked()
		for _, task := range queue.tasks {
			if task.status.State == SQLIndexRebuildRunning {
				task.status.CancelRequested = true
				if task.cancel != nil {
					task.cancel()
				}
			}
		}
		queue.updateIdleLocked(wasIdle)
		queue.signalWakeLocked()
	}
	queue.mu.Unlock()
	queue.workers.Wait()
	return nil
}

func (queue *SQLIndexRebuildQueue) worker() {
	defer queue.workers.Done()
	for {
		task, ok := queue.nextTask()
		if !ok {
			return
		}
		var runErr error
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					runErr = fmt.Errorf("index rebuild panic: %v", recovered)
				}
			}()
			runErr = task.request.Run(task.ctx, func(completed, total int) {
				queue.updateProgress(task, completed, total)
			})
		}()
		queue.finishTask(task, runErr)
	}
}

func (queue *SQLIndexRebuildQueue) nextTask() (*sqlIndexRebuildTask, bool) {
	for {
		queue.mu.Lock()
		if queue.ctx == nil || queue.closed {
			queue.mu.Unlock()
			return nil, false
		}
		if queue.ctx.Err() != nil {
			queue.stopFromContextLocked()
			queue.mu.Unlock()
			return nil, false
		}
		if queue.pending.Len() > 0 {
			task := heap.Pop(&queue.pending).(*sqlIndexRebuildTask)
			task.status.State = SQLIndexRebuildRunning
			task.status.StartedAt = time.Now()
			task.ctx, task.cancel = context.WithCancel(queue.ctx)
			queue.running++
			queue.mu.Unlock()
			return task, true
		}
		wake := queue.wake
		ctx := queue.ctx
		queue.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
		}
	}
}

func (queue *SQLIndexRebuildQueue) updateProgress(task *sqlIndexRebuildTask, completed, total int) {
	if completed < 0 || total < 0 || completed > total {
		return
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	current, ok := queue.tasks[task.request.ID]
	if !ok || current != task || task.status.State != SQLIndexRebuildRunning {
		return
	}
	if task.status.Total != 0 && total != task.status.Total {
		return
	}
	if completed < task.status.Completed {
		return
	}
	task.status.Total = total
	task.status.Completed = completed
}

func (queue *SQLIndexRebuildQueue) finishTask(task *sqlIndexRebuildTask, runErr error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if _, ok := queue.tasks[task.request.ID]; !ok || task.status.State != SQLIndexRebuildRunning {
		return
	}
	state := SQLIndexRebuildSucceeded
	errText := ""
	if runErr != nil {
		errText = runErr.Error()
	}
	if task.status.CancelRequested || task.ctx.Err() != nil || queue.ctx.Err() != nil {
		state = SQLIndexRebuildCanceled
		errText = ""
	} else if runErr != nil {
		state = SQLIndexRebuildFailed
	}
	queue.finishLocked(task, state, errText)
	queue.updateIdleLocked(false)
}

func (queue *SQLIndexRebuildQueue) finishLocked(task *sqlIndexRebuildTask, state SQLIndexRebuildState, errText string) {
	if task.status.State == SQLIndexRebuildRunning {
		queue.running--
	}
	delete(queue.tasks, task.request.ID)
	task.status.State = state
	task.status.FinishedAt = time.Now()
	task.status.Error = errText
	queue.history[task.request.ID] = sqlIndexRebuildHistoryEntry{status: task.status, sequence: task.sequence}
	queue.order = append(queue.order, task.request.ID)
	for len(queue.order) > queue.options.HistoryCapacity {
		oldest := queue.order[0]
		queue.order = queue.order[1:]
		delete(queue.history, oldest)
	}
}

func (queue *SQLIndexRebuildQueue) cancelPendingLocked() {
	for queue.pending.Len() > 0 {
		task := heap.Pop(&queue.pending).(*sqlIndexRebuildTask)
		task.status.CancelRequested = true
		queue.finishLocked(task, SQLIndexRebuildCanceled, "")
	}
}

func (queue *SQLIndexRebuildQueue) stopFromContextLocked() {
	queue.closed = true
	wasIdle := queue.isIdleLocked()
	queue.cancelPendingLocked()
	for _, task := range queue.tasks {
		if task.status.State == SQLIndexRebuildRunning {
			task.status.CancelRequested = true
			if task.cancel != nil {
				task.cancel()
			}
		}
	}
	queue.updateIdleLocked(wasIdle)
}

func (queue *SQLIndexRebuildQueue) isIdleLocked() bool {
	return len(queue.tasks) == 0
}

func (queue *SQLIndexRebuildQueue) updateIdleLocked(wasIdle bool) {
	nowIdle := queue.isIdleLocked()
	if wasIdle && !nowIdle {
		queue.idle = make(chan struct{})
		queue.idleClosed = false
	} else if !wasIdle && nowIdle && !queue.idleClosed {
		close(queue.idle)
		queue.idleClosed = true
	}
}

func (queue *SQLIndexRebuildQueue) signalWakeLocked() {
	wakeCount := queue.options.Workers
	if wakeCount == 0 {
		wakeCount = 1
	}
	for index := 0; index < wakeCount; index++ {
		select {
		case queue.wake <- struct{}{}:
		default:
			return
		}
	}
}
