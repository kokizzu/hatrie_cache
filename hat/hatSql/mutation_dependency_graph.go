package hatSql

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSQLMutationDependencyGraphMaxTasks bounds a graph created with a
	// zero maximum. The graph is opt-in and does not affect SQL execution.
	DefaultSQLMutationDependencyGraphMaxTasks = 4096
	maxSQLMutationDependencyGraphTasks        = 1 << 20
	maxSQLMutationTaskIDBytes                 = 256
	maxSQLMutationTaskErrorBytes              = 1024
	maxSQLMutationDependencySnapshotBytes     = 4 << 20
	sQLMutationDependencySnapshotVersion      = 1
)

var (
	// ErrSQLMutationDependencyGraphNil reports a method call on a nil graph.
	ErrSQLMutationDependencyGraphNil = errors.New("SQL mutation dependency graph is nil")
	// ErrSQLMutationDependencyGraphInvalid reports malformed task or graph
	// configuration.
	ErrSQLMutationDependencyGraphInvalid = errors.New("SQL mutation dependency graph is invalid")
	// ErrSQLMutationDependencyGraphDuplicate reports a repeated task or edge.
	ErrSQLMutationDependencyGraphDuplicate = errors.New("SQL mutation dependency graph task is duplicated")
	// ErrSQLMutationDependencyGraphMissingDependency reports an unknown task
	// dependency.
	ErrSQLMutationDependencyGraphMissingDependency = errors.New("SQL mutation dependency graph dependency is missing")
	// ErrSQLMutationDependencyGraphCapacity reports a graph at its configured
	// task limit.
	ErrSQLMutationDependencyGraphCapacity = errors.New("SQL mutation dependency graph capacity exceeded")
	// ErrSQLMutationDependencyGraphTaskNotFound reports an unknown task ID.
	ErrSQLMutationDependencyGraphTaskNotFound = errors.New("SQL mutation dependency graph task is not found")
	// ErrSQLMutationDependencyGraphState reports an invalid state transition.
	ErrSQLMutationDependencyGraphState = errors.New("SQL mutation dependency graph task state is invalid")
	// ErrSQLMutationDependencyGraphStaleAttempt reports a completion or failure
	// from a worker that no longer owns the current attempt.
	ErrSQLMutationDependencyGraphStaleAttempt = errors.New("SQL mutation dependency graph task attempt is stale")
	// ErrSQLMutationDependencyGraphCycle reports a cycle in a restored graph.
	ErrSQLMutationDependencyGraphCycle = errors.New("SQL mutation dependency graph contains a cycle")
	// ErrSQLMutationDependencyGraphSnapshot reports an invalid persisted graph
	// snapshot or an oversized snapshot.
	ErrSQLMutationDependencyGraphSnapshot = errors.New("SQL mutation dependency graph snapshot is invalid")
)

// SQLMutationTaskState is the durable lifecycle state of one mutation task.
type SQLMutationTaskState string

const (
	SQLMutationTaskPending   SQLMutationTaskState = "pending"
	SQLMutationTaskRunning   SQLMutationTaskState = "running"
	SQLMutationTaskCompleted SQLMutationTaskState = "completed"
	SQLMutationTaskFailed    SQLMutationTaskState = "failed"
)

// SQLMutationTask identifies one application-owned mutation and the tasks that
// must complete before it can run. IDs should be stable across restarts.
type SQLMutationTask struct {
	ID        string   `json:"id"`
	DependsOn []string `json:"depends_on,omitempty"`
}

// SQLMutationTaskRecord is the immutable copy returned by graph operations.
// Attempt is incremented every time a pending task is claimed.
type SQLMutationTaskRecord struct {
	ID        string               `json:"id"`
	DependsOn []string             `json:"depends_on,omitempty"`
	State     SQLMutationTaskState `json:"state"`
	Attempt   uint64               `json:"attempt"`
	LastError string               `json:"last_error,omitempty"`
}

// SQLMutationDependencyGraphSnapshot is the versioned persisted graph format.
type SQLMutationDependencyGraphSnapshot struct {
	Version uint32                  `json:"version"`
	Tasks   []SQLMutationTaskRecord `json:"tasks"`
}

// SQLMutationDependencyGraph coordinates application-owned mutation tasks.
// It is bounded, concurrency-safe, deterministic by task ID, and independent
// of the default SQL executor. Persist Snapshot through Save and call
// RequeueRunning after recovering from a process that may have lost workers.
type SQLMutationDependencyGraph struct {
	mu       sync.RWMutex
	maxTasks int
	tasks    map[string]*SQLMutationTaskRecord
}

// NewSQLMutationDependencyGraph creates an empty graph. A zero maximum uses
// DefaultSQLMutationDependencyGraphMaxTasks; negative or excessively large
// limits are rejected.
func NewSQLMutationDependencyGraph(maxTasks int) (*SQLMutationDependencyGraph, error) {
	if maxTasks < 0 || maxTasks > maxSQLMutationDependencyGraphTasks {
		return nil, fmt.Errorf("%w: maximum tasks must be between 0 and %d", ErrSQLMutationDependencyGraphInvalid, maxSQLMutationDependencyGraphTasks)
	}
	if maxTasks == 0 {
		maxTasks = DefaultSQLMutationDependencyGraphMaxTasks
	}
	return &SQLMutationDependencyGraph{
		maxTasks: maxTasks,
		tasks:    make(map[string]*SQLMutationTaskRecord),
	}, nil
}

// Add registers a pending task. Dependencies must already be registered and
// cannot be changed after the task is added.
func (graph *SQLMutationDependencyGraph) Add(task SQLMutationTask) error {
	if graph == nil {
		return ErrSQLMutationDependencyGraphNil
	}
	id, dependencies, err := normalizeSQLMutationTask(task)
	if err != nil {
		return err
	}
	graph.mu.Lock()
	defer graph.mu.Unlock()
	if _, exists := graph.tasks[id]; exists {
		return fmt.Errorf("%w: %q", ErrSQLMutationDependencyGraphDuplicate, id)
	}
	if len(graph.tasks) >= graph.maxTasks {
		return fmt.Errorf("%w: maximum is %d", ErrSQLMutationDependencyGraphCapacity, graph.maxTasks)
	}
	for _, dependency := range dependencies {
		if _, exists := graph.tasks[dependency]; !exists {
			return fmt.Errorf("%w: task %q depends on %q", ErrSQLMutationDependencyGraphMissingDependency, id, dependency)
		}
	}
	graph.tasks[id] = &SQLMutationTaskRecord{
		ID:        id,
		DependsOn: dependencies,
		State:     SQLMutationTaskPending,
	}
	return nil
}

// ClaimReady atomically claims up to limit ready tasks in deterministic ID
// order. A non-positive limit claims every currently ready task. Claimed
// attempts remain owned by the caller until Complete or Fail is called.
func (graph *SQLMutationDependencyGraph) ClaimReady(limit int) []SQLMutationTaskRecord {
	if graph == nil {
		return nil
	}
	graph.mu.Lock()
	defer graph.mu.Unlock()
	ids := make([]string, 0, len(graph.tasks))
	for id, task := range graph.tasks {
		if task.State == SQLMutationTaskPending && graph.dependenciesCompletedLocked(task) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	claimed := make([]SQLMutationTaskRecord, 0, len(ids))
	for _, id := range ids {
		task := graph.tasks[id]
		task.State = SQLMutationTaskRunning
		task.Attempt++
		claimed = append(claimed, cloneSQLMutationTaskRecord(*task))
	}
	return claimed
}

// Complete marks an owned running task complete. The attempt token prevents a
// stale worker from completing a task after it has been requeued and claimed
// by another worker.
func (graph *SQLMutationDependencyGraph) Complete(id string, attempt uint64) error {
	return graph.finishSQLMutationTask(id, attempt, SQLMutationTaskCompleted, "")
}

// Fail marks an owned running task failed and records a bounded diagnostic.
// Call Retry after the failure has been handled and the task may run again.
func (graph *SQLMutationDependencyGraph) Fail(id string, attempt uint64, reason string) error {
	reason = strings.TrimSpace(reason)
	if len(reason) > maxSQLMutationTaskErrorBytes {
		return fmt.Errorf("%w: failure reason exceeds %d bytes", ErrSQLMutationDependencyGraphInvalid, maxSQLMutationTaskErrorBytes)
	}
	return graph.finishSQLMutationTask(id, attempt, SQLMutationTaskFailed, reason)
}

// Retry moves a failed task back to pending. Its previous diagnostic remains
// in the snapshot until the next successful completion.
func (graph *SQLMutationDependencyGraph) Retry(id string) error {
	if graph == nil {
		return ErrSQLMutationDependencyGraphNil
	}
	graph.mu.Lock()
	defer graph.mu.Unlock()
	task, exists := graph.tasks[id]
	if !exists {
		return fmt.Errorf("%w: %q", ErrSQLMutationDependencyGraphTaskNotFound, id)
	}
	if task.State != SQLMutationTaskFailed {
		return fmt.Errorf("%w: cannot retry task %q in state %q", ErrSQLMutationDependencyGraphState, id, task.State)
	}
	task.State = SQLMutationTaskPending
	return nil
}

// RequeueRunning returns running tasks to pending so a recovered process can
// safely claim work whose previous worker may have disappeared.
func (graph *SQLMutationDependencyGraph) RequeueRunning() int {
	if graph == nil {
		return 0
	}
	graph.mu.Lock()
	defer graph.mu.Unlock()
	count := 0
	for _, task := range graph.tasks {
		if task.State == SQLMutationTaskRunning {
			task.State = SQLMutationTaskPending
			count++
		}
	}
	return count
}

// Task returns a snapshot of one task.
func (graph *SQLMutationDependencyGraph) Task(id string) (SQLMutationTaskRecord, bool) {
	if graph == nil {
		return SQLMutationTaskRecord{}, false
	}
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	task, exists := graph.tasks[id]
	if !exists {
		return SQLMutationTaskRecord{}, false
	}
	return cloneSQLMutationTaskRecord(*task), true
}

// Snapshot returns all tasks sorted by ID and detached from graph-owned state.
func (graph *SQLMutationDependencyGraph) Snapshot() []SQLMutationTaskRecord {
	if graph == nil {
		return nil
	}
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	ids := make([]string, 0, len(graph.tasks))
	for id := range graph.tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	tasks := make([]SQLMutationTaskRecord, 0, len(ids))
	for _, id := range ids {
		tasks = append(tasks, cloneSQLMutationTaskRecord(*graph.tasks[id]))
	}
	return tasks
}

// Save writes a versioned JSON snapshot of the graph.
func (graph *SQLMutationDependencyGraph) Save(writer io.Writer) error {
	if graph == nil {
		return ErrSQLMutationDependencyGraphNil
	}
	if writer == nil {
		return fmt.Errorf("%w: writer is nil", ErrSQLMutationDependencyGraphSnapshot)
	}
	snapshot := SQLMutationDependencyGraphSnapshot{
		Version: sQLMutationDependencySnapshotVersion,
		Tasks:   graph.Snapshot(),
	}
	return json.NewEncoder(writer).Encode(snapshot)
}

// Load replaces the graph only after the complete snapshot has been decoded
// and validated. This makes malformed or cyclic recovery metadata harmless to
// the currently active graph.
func (graph *SQLMutationDependencyGraph) Load(reader io.Reader) error {
	if graph == nil {
		return ErrSQLMutationDependencyGraphNil
	}
	if reader == nil {
		return fmt.Errorf("%w: reader is nil", ErrSQLMutationDependencyGraphSnapshot)
	}
	limited := &io.LimitedReader{R: reader, N: maxSQLMutationDependencySnapshotBytes + 1}
	var snapshot SQLMutationDependencyGraphSnapshot
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(&snapshot); err != nil {
		return fmt.Errorf("%w: %v", ErrSQLMutationDependencyGraphSnapshot, err)
	}
	if limited.N <= 0 {
		return fmt.Errorf("%w: exceeds %d bytes", ErrSQLMutationDependencyGraphSnapshot, maxSQLMutationDependencySnapshotBytes)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("%w: multiple JSON values", ErrSQLMutationDependencyGraphSnapshot)
		}
		return fmt.Errorf("%w: trailing data: %v", ErrSQLMutationDependencyGraphSnapshot, err)
	}
	tasks, err := validateSQLMutationDependencySnapshot(snapshot)
	if err != nil {
		return err
	}
	graph.mu.Lock()
	defer graph.mu.Unlock()
	if len(tasks) > graph.maxTasks {
		return fmt.Errorf("%w: snapshot has %d tasks, maximum is %d", ErrSQLMutationDependencyGraphCapacity, len(tasks), graph.maxTasks)
	}
	graph.tasks = tasks
	return nil
}

func (graph *SQLMutationDependencyGraph) finishSQLMutationTask(id string, attempt uint64, state SQLMutationTaskState, reason string) error {
	if graph == nil {
		return ErrSQLMutationDependencyGraphNil
	}
	graph.mu.Lock()
	defer graph.mu.Unlock()
	task, exists := graph.tasks[id]
	if !exists {
		return fmt.Errorf("%w: %q", ErrSQLMutationDependencyGraphTaskNotFound, id)
	}
	if task.State != SQLMutationTaskRunning {
		return fmt.Errorf("%w: task %q is in state %q", ErrSQLMutationDependencyGraphState, id, task.State)
	}
	if attempt == 0 || task.Attempt != attempt {
		return fmt.Errorf("%w: task %q attempt %d", ErrSQLMutationDependencyGraphStaleAttempt, id, attempt)
	}
	task.State = state
	task.LastError = reason
	return nil
}

func (graph *SQLMutationDependencyGraph) dependenciesCompletedLocked(task *SQLMutationTaskRecord) bool {
	for _, dependency := range task.DependsOn {
		if graph.tasks[dependency].State != SQLMutationTaskCompleted {
			return false
		}
	}
	return true
}

func normalizeSQLMutationTask(task SQLMutationTask) (string, []string, error) {
	id := strings.TrimSpace(task.ID)
	if id == "" || len(id) > maxSQLMutationTaskIDBytes {
		return "", nil, fmt.Errorf("%w: task ID must contain 1 to %d bytes", ErrSQLMutationDependencyGraphInvalid, maxSQLMutationTaskIDBytes)
	}
	dependencies := make([]string, len(task.DependsOn))
	seen := make(map[string]struct{}, len(task.DependsOn))
	for index, raw := range task.DependsOn {
		dependency := strings.TrimSpace(raw)
		if dependency == "" || len(dependency) > maxSQLMutationTaskIDBytes {
			return "", nil, fmt.Errorf("%w: dependency ID must contain 1 to %d bytes", ErrSQLMutationDependencyGraphInvalid, maxSQLMutationTaskIDBytes)
		}
		if dependency == id {
			return "", nil, fmt.Errorf("%w: task %q depends on itself", ErrSQLMutationDependencyGraphCycle, id)
		}
		if _, exists := seen[dependency]; exists {
			return "", nil, fmt.Errorf("%w: task %q depends on %q twice", ErrSQLMutationDependencyGraphDuplicate, id, dependency)
		}
		seen[dependency] = struct{}{}
		dependencies[index] = dependency
	}
	sort.Strings(dependencies)
	return id, dependencies, nil
}

func cloneSQLMutationTaskRecord(task SQLMutationTaskRecord) SQLMutationTaskRecord {
	task.DependsOn = append([]string(nil), task.DependsOn...)
	return task
}

func validateSQLMutationDependencySnapshot(snapshot SQLMutationDependencyGraphSnapshot) (map[string]*SQLMutationTaskRecord, error) {
	if snapshot.Version != sQLMutationDependencySnapshotVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrSQLMutationDependencyGraphSnapshot, snapshot.Version)
	}
	tasks := make(map[string]*SQLMutationTaskRecord, len(snapshot.Tasks))
	for _, raw := range snapshot.Tasks {
		id, dependencies, err := normalizeSQLMutationTask(SQLMutationTask{ID: raw.ID, DependsOn: raw.DependsOn})
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrSQLMutationDependencyGraphSnapshot, err)
		}
		if _, exists := tasks[id]; exists {
			return nil, fmt.Errorf("%w: duplicate task %q", ErrSQLMutationDependencyGraphSnapshot, id)
		}
		if !validSQLMutationTaskState(raw.State) {
			return nil, fmt.Errorf("%w: task %q has state %q", ErrSQLMutationDependencyGraphSnapshot, id, raw.State)
		}
		if raw.State != SQLMutationTaskPending && raw.Attempt == 0 {
			return nil, fmt.Errorf("%w: task %q has zero attempt in state %q", ErrSQLMutationDependencyGraphSnapshot, id, raw.State)
		}
		if len(raw.LastError) > maxSQLMutationTaskErrorBytes {
			return nil, fmt.Errorf("%w: task %q failure reason exceeds %d bytes", ErrSQLMutationDependencyGraphSnapshot, id, maxSQLMutationTaskErrorBytes)
		}
		tasks[id] = &SQLMutationTaskRecord{
			ID:        id,
			DependsOn: dependencies,
			State:     raw.State,
			Attempt:   raw.Attempt,
			LastError: raw.LastError,
		}
	}
	for id, task := range tasks {
		for _, dependency := range task.DependsOn {
			if _, exists := tasks[dependency]; !exists {
				return nil, fmt.Errorf("%w: task %q depends on %q", ErrSQLMutationDependencyGraphSnapshot, id, dependency)
			}
		}
	}
	colors := make(map[string]uint8, len(tasks))
	var visit func(string) error
	visit = func(id string) error {
		switch colors[id] {
		case 1:
			return fmt.Errorf("%w: task %q", ErrSQLMutationDependencyGraphCycle, id)
		case 2:
			return nil
		}
		colors[id] = 1
		for _, dependency := range tasks[id].DependsOn {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		colors[id] = 2
		return nil
	}
	ids := make([]string, 0, len(tasks))
	for id := range tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := visit(id); err != nil {
			return nil, err
		}
	}
	return tasks, nil
}

func validSQLMutationTaskState(state SQLMutationTaskState) bool {
	switch state {
	case SQLMutationTaskPending, SQLMutationTaskRunning, SQLMutationTaskCompleted, SQLMutationTaskFailed:
		return true
	default:
		return false
	}
}
