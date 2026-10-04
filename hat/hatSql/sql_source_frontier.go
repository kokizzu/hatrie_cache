package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrSQLSourceFrontierTrackerNil        = errors.New("hatSql: SQL source frontier tracker is nil")
	ErrSQLSourceFrontierInvalidPartitions = errors.New("hatSql: SQL source frontier partitions are invalid")
	ErrSQLSourceFrontierUnknownPartition  = errors.New("hatSql: SQL source frontier partition is unknown")
	ErrSQLSourceFrontierDuplicate         = errors.New("hatSql: SQL source frontier update is duplicated")
)

// SQLSourceFrontierPartition identifies one fixed input partition participating
// in a common snapshot frontier.
type SQLSourceFrontierPartition struct {
	Source    string
	Partition string
}

// SQLSourceFrontier is one monotone source-partition frontier observation.
// Frontier zero is valid; readiness is tracked separately from its value.
type SQLSourceFrontier struct {
	Source    string
	Partition string
	Frontier  uint64
}

// SQLSourceFrontierSnapshot is an independently owned observation snapshot.
type SQLSourceFrontierSnapshot struct {
	Source    string
	Partition string
	Frontier  uint64
	Observed  bool
}

type sqlSourceFrontierKey struct {
	source    string
	partition string
}

type sqlSourceFrontierState struct {
	partition SQLSourceFrontierPartition
	frontier  uint64
	observed  bool
	heapIndex int
}

// SQLSourceFrontierTracker coordinates a fixed set of source partitions. It
// keeps the minimum observed frontier at the root of an indexed min-heap, so
// CommonFrontier and ReadyAt are O(1). The tracker is opt-in and has no effect
// on ordinary SourceResolver or SQL query execution.
type SQLSourceFrontierTracker struct {
	mu            sync.RWMutex
	states        []sqlSourceFrontierState
	indexes       map[sqlSourceFrontierKey]int
	frontierHeap  []int
	observedCount int
	notify        chan struct{}
	waiters       int
}

// NewSQLSourceFrontierTracker creates a tracker for the supplied fixed source
// partitions. Partition names are trimmed, sorted for deterministic snapshots,
// and rejected when empty or duplicated.
func NewSQLSourceFrontierTracker(partitions []SQLSourceFrontierPartition) (*SQLSourceFrontierTracker, error) {
	if len(partitions) == 0 {
		return nil, ErrSQLSourceFrontierInvalidPartitions
	}
	normalized := make([]SQLSourceFrontierPartition, len(partitions))
	seen := make(map[sqlSourceFrontierKey]struct{}, len(partitions))
	for index, partition := range partitions {
		partition.Source = strings.TrimSpace(partition.Source)
		partition.Partition = strings.TrimSpace(partition.Partition)
		if partition.Source == "" || partition.Partition == "" {
			return nil, fmt.Errorf("%w: source and partition are required", ErrSQLSourceFrontierInvalidPartitions)
		}
		key := sqlSourceFrontierKey{source: partition.Source, partition: partition.Partition}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("%w: %s/%s", ErrSQLSourceFrontierInvalidPartitions, partition.Source, partition.Partition)
		}
		seen[key] = struct{}{}
		normalized[index] = partition
	}
	sort.Slice(normalized, func(left, right int) bool {
		if normalized[left].Source != normalized[right].Source {
			return normalized[left].Source < normalized[right].Source
		}
		return normalized[left].Partition < normalized[right].Partition
	})

	tracker := &SQLSourceFrontierTracker{
		states:       make([]sqlSourceFrontierState, len(normalized)),
		indexes:      make(map[sqlSourceFrontierKey]int, len(normalized)),
		frontierHeap: make([]int, len(normalized)),
	}
	for index, partition := range normalized {
		tracker.states[index] = sqlSourceFrontierState{partition: partition, heapIndex: index}
		tracker.indexes[sqlSourceFrontierKey{source: partition.Source, partition: partition.Partition}] = index
		tracker.frontierHeap[index] = index
	}
	return tracker, nil
}

// Observe records a newer partition frontier. Older and equal observations
// are idempotent no-ops and return false.
func (tracker *SQLSourceFrontierTracker) Observe(frontier SQLSourceFrontier) (bool, error) {
	if tracker == nil {
		return false, ErrSQLSourceFrontierTrackerNil
	}
	key, normalized, err := normalizeSQLSourceFrontier(frontier)
	if err != nil {
		return false, err
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	index, known := tracker.indexes[key]
	if !known {
		return false, fmt.Errorf("%w: %s/%s", ErrSQLSourceFrontierUnknownPartition, normalized.Source, normalized.Partition)
	}
	changed := tracker.observeLocked(index, normalized.Frontier)
	if changed {
		tracker.signalWaitersLocked()
	}
	return changed, nil
}

// ObserveBatch validates and observes distinct partitions atomically. Stale
// observations are valid no-ops; an unknown or duplicate partition leaves all
// tracker state unchanged.
func (tracker *SQLSourceFrontierTracker) ObserveBatch(frontiers []SQLSourceFrontier) (int, error) {
	if tracker == nil {
		return 0, ErrSQLSourceFrontierTrackerNil
	}
	if len(frontiers) == 0 {
		return 0, nil
	}
	normalized := make([]SQLSourceFrontier, len(frontiers))
	seen := make(map[sqlSourceFrontierKey]struct{}, len(frontiers))
	for index, frontier := range frontiers {
		key, value, err := normalizeSQLSourceFrontier(frontier)
		if err != nil {
			return 0, err
		}
		if _, exists := seen[key]; exists {
			return 0, ErrSQLSourceFrontierDuplicate
		}
		seen[key] = struct{}{}
		normalized[index] = value
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	indexes := make([]int, len(normalized))
	for index, frontier := range normalized {
		key := sqlSourceFrontierKey{source: frontier.Source, partition: frontier.Partition}
		stateIndex, known := tracker.indexes[key]
		if !known {
			return 0, fmt.Errorf("%w: %s/%s", ErrSQLSourceFrontierUnknownPartition, frontier.Source, frontier.Partition)
		}
		indexes[index] = stateIndex
	}
	changed := 0
	for index, frontier := range normalized {
		if tracker.observeLocked(indexes[index], frontier.Frontier) {
			changed++
		}
	}
	if changed > 0 {
		tracker.signalWaitersLocked()
	}
	return changed, nil
}

// CommonFrontier returns the minimum frontier after every configured
// partition has been observed. The ready result distinguishes an unobserved
// zero frontier from a valid observed zero frontier.
func (tracker *SQLSourceFrontierTracker) CommonFrontier() (frontier uint64, ready bool) {
	if tracker == nil {
		return 0, false
	}
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	if tracker.observedCount != len(tracker.states) || len(tracker.frontierHeap) == 0 {
		return 0, false
	}
	return tracker.states[tracker.frontierHeap[0]].frontier, true
}

// ReadyAt reports whether every configured partition has reached frontier.
func (tracker *SQLSourceFrontierTracker) ReadyAt(frontier uint64) bool {
	if tracker == nil {
		return false
	}
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	return tracker.readyAtLocked(frontier)
}

// WaitReady blocks until every configured partition has reached frontier or
// ctx is canceled. It is an opt-in admission barrier for snapshot-dependent
// readers; ordinary observation and readiness checks remain allocation-free.
func (tracker *SQLSourceFrontierTracker) WaitReady(ctx context.Context, frontier uint64) error {
	if tracker == nil {
		return ErrSQLSourceFrontierTrackerNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		tracker.mu.Lock()
		if tracker.readyAtLocked(frontier) {
			tracker.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			tracker.mu.Unlock()
			return err
		}
		if tracker.notify == nil {
			tracker.notify = make(chan struct{})
		}
		wait := tracker.notify
		tracker.waiters++
		tracker.mu.Unlock()
		select {
		case <-ctx.Done():
			tracker.removeWaiter()
			return ctx.Err()
		case <-wait:
			tracker.removeWaiter()
		}
	}
}

// Frontier returns one partition's latest observed frontier.
func (tracker *SQLSourceFrontierTracker) Frontier(source, partition string) (uint64, bool) {
	if tracker == nil {
		return 0, false
	}
	source = strings.TrimSpace(source)
	partition = strings.TrimSpace(partition)
	if source == "" || partition == "" {
		return 0, false
	}
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	index, known := tracker.indexes[sqlSourceFrontierKey{source: source, partition: partition}]
	if !known || !tracker.states[index].observed {
		return 0, false
	}
	return tracker.states[index].frontier, true
}

// Snapshot returns all configured partitions in deterministic order.
func (tracker *SQLSourceFrontierTracker) Snapshot() []SQLSourceFrontierSnapshot {
	if tracker == nil {
		return nil
	}
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	snapshot := make([]SQLSourceFrontierSnapshot, len(tracker.states))
	for index, state := range tracker.states {
		snapshot[index] = SQLSourceFrontierSnapshot{
			Source:    state.partition.Source,
			Partition: state.partition.Partition,
			Frontier:  state.frontier,
			Observed:  state.observed,
		}
	}
	return snapshot
}

func (tracker *SQLSourceFrontierTracker) observeLocked(index int, frontier uint64) bool {
	state := &tracker.states[index]
	if state.observed && frontier <= state.frontier {
		return false
	}
	if !state.observed {
		state.observed = true
		tracker.observedCount++
	}
	state.frontier = frontier
	tracker.frontierDown(state.heapIndex)
	return true
}

func (tracker *SQLSourceFrontierTracker) readyAtLocked(frontier uint64) bool {
	return tracker.observedCount == len(tracker.states) && len(tracker.frontierHeap) > 0 && tracker.states[tracker.frontierHeap[0]].frontier >= frontier
}

func (tracker *SQLSourceFrontierTracker) signalWaitersLocked() {
	if tracker.notify == nil || tracker.waiters == 0 {
		return
	}
	close(tracker.notify)
	tracker.notify = make(chan struct{})
}

func (tracker *SQLSourceFrontierTracker) removeWaiter() {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.waiters--
	if tracker.waiters == 0 {
		tracker.notify = nil
	}
}

func (tracker *SQLSourceFrontierTracker) frontierDown(index int) {
	for {
		left := index*2 + 1
		if left >= len(tracker.frontierHeap) {
			return
		}
		smallest := left
		right := left + 1
		if right < len(tracker.frontierHeap) && tracker.frontierLess(right, left) {
			smallest = right
		}
		if !tracker.frontierLess(smallest, index) {
			return
		}
		tracker.frontierSwap(index, smallest)
		index = smallest
	}
}

func (tracker *SQLSourceFrontierTracker) frontierLess(left, right int) bool {
	leftState := tracker.states[tracker.frontierHeap[left]]
	rightState := tracker.states[tracker.frontierHeap[right]]
	if leftState.frontier != rightState.frontier {
		return leftState.frontier < rightState.frontier
	}
	return tracker.frontierHeap[left] < tracker.frontierHeap[right]
}

func (tracker *SQLSourceFrontierTracker) frontierSwap(left, right int) {
	tracker.frontierHeap[left], tracker.frontierHeap[right] = tracker.frontierHeap[right], tracker.frontierHeap[left]
	tracker.states[tracker.frontierHeap[left]].heapIndex = left
	tracker.states[tracker.frontierHeap[right]].heapIndex = right
}

func normalizeSQLSourceFrontier(frontier SQLSourceFrontier) (sqlSourceFrontierKey, SQLSourceFrontier, error) {
	frontier.Source = strings.TrimSpace(frontier.Source)
	frontier.Partition = strings.TrimSpace(frontier.Partition)
	if frontier.Source == "" || frontier.Partition == "" {
		return sqlSourceFrontierKey{}, SQLSourceFrontier{}, ErrSQLSourceFrontierInvalidPartitions
	}
	return sqlSourceFrontierKey{source: frontier.Source, partition: frontier.Partition}, frontier, nil
}
