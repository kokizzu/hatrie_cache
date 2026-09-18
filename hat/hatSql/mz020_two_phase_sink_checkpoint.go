package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSQLSinkTwoPhaseCapacity bounds retained sink transactions when no
	// explicit capacity is configured.
	DefaultSQLSinkTwoPhaseCapacity = 1024
	// MaxSQLSinkTwoPhaseCapacity prevents an accidental unbounded checkpoint.
	MaxSQLSinkTwoPhaseCapacity = 65536
	// MaxSQLSinkTwoPhasePartitions bounds retained sink partition frontiers.
	MaxSQLSinkTwoPhasePartitions  = MaxSQLSinkExactlyOncePartitions
	maxSQLSinkTwoPhaseStringBytes = maxSQLSinkExactlyOnceStringBytes
)

var (
	// ErrSQLSinkTwoPhaseNil reports a method call on a nil coordinator.
	ErrSQLSinkTwoPhaseNil = errors.New("SQL sink two-phase coordinator is nil")
	// ErrSQLSinkTwoPhaseInvalid reports malformed commit or checkpoint data.
	ErrSQLSinkTwoPhaseInvalid = errors.New("SQL sink two-phase data is invalid")
	// ErrSQLSinkTwoPhaseConflict reports reuse of a transaction or key with a
	// different commit definition.
	ErrSQLSinkTwoPhaseConflict = errors.New("SQL sink two-phase commit conflicts with existing state")
	// ErrSQLSinkTwoPhaseStale reports a progress value that cannot advance a
	// committed sink frontier.
	ErrSQLSinkTwoPhaseStale = errors.New("SQL sink two-phase progress is stale")
	// ErrSQLSinkTwoPhaseNotPrepared reports a phase-two call without a durable
	// phase-one checkpoint.
	ErrSQLSinkTwoPhaseNotPrepared = errors.New("SQL sink two-phase commit is not prepared")
	// ErrSQLSinkTwoPhaseInFlight reports a partition reserved by another
	// prepared transaction.
	ErrSQLSinkTwoPhaseInFlight = errors.New("SQL sink two-phase partition is in flight")
	// ErrSQLSinkTwoPhaseCapacity reports that no committed transaction can be
	// evicted to make room for a new prepared transaction.
	ErrSQLSinkTwoPhaseCapacity = errors.New("SQL sink two-phase coordinator is at capacity")
	// ErrSQLSinkTwoPhaseCheckpointInFlight reports restore during a callback.
	ErrSQLSinkTwoPhaseCheckpointInFlight = errors.New("SQL sink two-phase checkpoint has an in-flight operation")
	// ErrSQLSinkTwoPhaseParticipantRequired reports a missing sink participant.
	ErrSQLSinkTwoPhaseParticipantRequired = errors.New("SQL sink two-phase participant is required")
	// ErrSQLSinkTwoPhaseParticipantPanic reports a callback panic converted to a
	// retryable operation error.
	ErrSQLSinkTwoPhaseParticipantPanic = errors.New("SQL sink two-phase participant panicked")
)

// SQLSinkTwoPhaseState is the durable phase of one sink transaction.
type SQLSinkTwoPhaseState string

const (
	// SQLSinkTwoPhasePrepared means the external sink has staged the effect and
	// the prepared checkpoint is durable, but progress is not acknowledged.
	SQLSinkTwoPhasePrepared SQLSinkTwoPhaseState = "prepared"
	// SQLSinkTwoPhaseCommitted means the external sink has published the effect
	// and the progress checkpoint is durable.
	SQLSinkTwoPhaseCommitted SQLSinkTwoPhaseState = "committed"
)

// SQLSinkTwoPhaseCheckpoint is the durable state of a two-phase sink
// coordinator. Prepared entries are retained for crash recovery; only
// committed entries contribute to Progress.
type SQLSinkTwoPhaseCheckpoint struct {
	Capacity     int                    `json:"capacity"`
	NextSequence uint64                 `json:"next_sequence"`
	Entries      []SQLSinkTwoPhaseEntry `json:"entries"`
	Progress     []SQLSinkProgress      `json:"progress"`
}

// SQLSinkTwoPhaseEntry is one ordered sink transaction in a checkpoint.
type SQLSinkTwoPhaseEntry struct {
	Sequence uint64               `json:"sequence"`
	Commit   SQLSinkCommit        `json:"commit"`
	State    SQLSinkTwoPhaseState `json:"state"`
}

// SQLSinkTwoPhaseCheckpointStore durably loads and replaces one named
// checkpoint. Save must be atomic and durable before returning. A store must
// preserve a prepared entry after a process crash so the caller can retry the
// participant's Commit operation.
type SQLSinkTwoPhaseCheckpointStore interface {
	LoadSQLSinkTwoPhaseCheckpoint(context.Context, string) (SQLSinkTwoPhaseCheckpoint, bool, error)
	SaveSQLSinkTwoPhaseCheckpoint(context.Context, string, SQLSinkTwoPhaseCheckpoint) error
}

// SQLSinkTwoPhaseParticipant stages and publishes one external sink effect.
// Both operations must be idempotent for the supplied key. Prepare must not
// make the effect visible; Commit may publish it.
type SQLSinkTwoPhaseParticipant interface {
	Prepare(context.Context, string) error
	Commit(context.Context, string) error
}

// SQLSinkTwoPhaseOptions configures bounded retention and optional durable
// recovery. A nil CheckpointStore keeps the coordinator in memory.
type SQLSinkTwoPhaseOptions struct {
	Capacity        int
	CheckpointStore SQLSinkTwoPhaseCheckpointStore
	Name            string
}

type sqlSinkTwoPhaseState struct {
	entry     SQLSinkTwoPhaseEntry
	durable   bool
	busy      bool
	attempt   *sqlSinkTwoPhaseAttempt
	evictions []sqlSinkExactlyOnceKey
}

type sqlSinkTwoPhaseAttempt struct {
	done chan struct{}
	err  error
}

// SQLSinkTwoPhaseCoordinator coordinates durable prepare, external publish,
// and durable commit phases. It is opt-in and does not change the existing
// one-phase SQL sink APIs.
type SQLSinkTwoPhaseCoordinator struct {
	mu                 sync.Mutex
	finalizeMu         sync.Mutex
	capacity           int
	nextSequence       uint64
	entries            map[sqlSinkExactlyOnceKey]*sqlSinkTwoPhaseState
	transactions       map[sqlSinkExactlyOnceTransactionKey]sqlSinkExactlyOnceKey
	order              []sqlSinkExactlyOnceKey
	frontiers          map[sqlSinkProgressKey]uint64
	inFlightPartitions map[sqlSinkProgressKey]struct{}
	pendingEvictions   map[sqlSinkExactlyOnceKey]struct{}
	store              SQLSinkTwoPhaseCheckpointStore
	name               string
}

// NewSQLSinkTwoPhaseCoordinator creates a coordinator and restores its named
// checkpoint when a store is configured. A missing checkpoint starts empty.
func NewSQLSinkTwoPhaseCoordinator(ctx context.Context, options SQLSinkTwoPhaseOptions) (*SQLSinkTwoPhaseCoordinator, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	capacity := normalizeSQLSinkTwoPhaseCapacity(options.Capacity)
	name := strings.TrimSpace(options.Name)
	if options.CheckpointStore != nil {
		if err := validateSQLSinkTwoPhaseName(name); err != nil {
			return nil, err
		}
	}
	coordinator := newSQLSinkTwoPhaseCoordinator(capacity, options.CheckpointStore, name)
	if options.CheckpointStore == nil {
		return coordinator, nil
	}
	snapshot, found, err := options.CheckpointStore.LoadSQLSinkTwoPhaseCheckpoint(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("load SQL sink two-phase checkpoint %q: %w", name, err)
	}
	if !found {
		return coordinator, nil
	}
	if err := coordinator.Restore(snapshot); err != nil {
		return nil, fmt.Errorf("restore SQL sink two-phase checkpoint %q: %w", name, err)
	}
	return coordinator, nil
}

// NewSQLSinkTwoPhaseCoordinatorFromSnapshot creates an in-memory coordinator
// from a validated checkpoint.
func NewSQLSinkTwoPhaseCoordinatorFromSnapshot(snapshot SQLSinkTwoPhaseCheckpoint) (*SQLSinkTwoPhaseCoordinator, error) {
	coordinator := newSQLSinkTwoPhaseCoordinator(normalizeSQLSinkTwoPhaseCapacity(snapshot.Capacity), nil, "")
	if err := coordinator.Restore(snapshot); err != nil {
		return nil, err
	}
	return coordinator, nil
}

func newSQLSinkTwoPhaseCoordinator(capacity int, store SQLSinkTwoPhaseCheckpointStore, name string) *SQLSinkTwoPhaseCoordinator {
	return &SQLSinkTwoPhaseCoordinator{
		capacity:           capacity,
		nextSequence:       1,
		entries:            make(map[sqlSinkExactlyOnceKey]*sqlSinkTwoPhaseState),
		transactions:       make(map[sqlSinkExactlyOnceTransactionKey]sqlSinkExactlyOnceKey),
		frontiers:          make(map[sqlSinkProgressKey]uint64),
		inFlightPartitions: make(map[sqlSinkProgressKey]struct{}),
		pendingEvictions:   make(map[sqlSinkExactlyOnceKey]struct{}),
		store:              store,
		name:               name,
	}
}

// Prepare stages a sink effect and durably records the prepared checkpoint.
// It returns true only when the participant was called for a new transaction.
func (coordinator *SQLSinkTwoPhaseCoordinator) Prepare(ctx context.Context, commit SQLSinkCommit, participant SQLSinkTwoPhaseParticipant) (bool, error) {
	if coordinator == nil {
		return false, ErrSQLSinkTwoPhaseNil
	}
	if participant == nil {
		return false, ErrSQLSinkTwoPhaseParticipantRequired
	}
	ctx = normalizeSQLSinkTwoPhaseContext(ctx)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key, normalized, err := normalizeSQLSinkTwoPhaseCommit(commit)
	if err != nil {
		return false, err
	}

	for {
		coordinator.mu.Lock()
		coordinator.ensureLocked()
		state, found := coordinator.entries[key]
		if found {
			if !equalSQLSinkExactlyOnceCommit(state.entry.Commit, normalized) {
				coordinator.mu.Unlock()
				return false, ErrSQLSinkTwoPhaseConflict
			}
			if state.busy {
				attempt := state.attempt
				coordinator.mu.Unlock()
				if err := waitSQLSinkTwoPhaseAttempt(ctx, attempt); err != nil {
					return false, err
				}
				continue
			}
			coordinator.mu.Unlock()
			return false, nil
		}
		transactionKey := sqlSinkExactlyOnceTransactionKey{sink: normalized.Sink, transactionID: normalized.TransactionID}
		if existingKey, exists := coordinator.transactions[transactionKey]; exists && existingKey != key {
			coordinator.mu.Unlock()
			return false, ErrSQLSinkTwoPhaseConflict
		}
		if err := coordinator.validateNewCommitLocked(normalized); err != nil {
			coordinator.mu.Unlock()
			return false, err
		}
		evictions, err := coordinator.planCapacityLocked()
		if err != nil {
			coordinator.mu.Unlock()
			return false, err
		}
		if coordinator.nextSequence == 0 || coordinator.nextSequence == ^uint64(0) {
			coordinator.mu.Unlock()
			return false, ErrSQLSinkTwoPhaseInvalid
		}
		attempt := &sqlSinkTwoPhaseAttempt{done: make(chan struct{})}
		state = &sqlSinkTwoPhaseState{
			entry: SQLSinkTwoPhaseEntry{
				Sequence: coordinator.nextSequence,
				Commit:   cloneSQLSinkCommit(normalized),
				State:    SQLSinkTwoPhasePrepared,
			},
			busy:      true,
			attempt:   attempt,
			evictions: evictions,
		}
		coordinator.nextSequence++
		coordinator.entries[key] = state
		coordinator.transactions[transactionKey] = key
		coordinator.order = append(coordinator.order, key)
		for _, progress := range normalized.Progress {
			coordinator.inFlightPartitions[sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}] = struct{}{}
		}
		coordinator.mu.Unlock()

		err = invokeSQLSinkTwoPhaseCallback(func() error {
			return participant.Prepare(ctx, normalized.IdempotencyKey)
		})
		if err != nil {
			coordinator.failPrepare(key, state, err)
			return false, err
		}
		if err := ctx.Err(); err != nil {
			coordinator.failPrepare(key, state, err)
			return false, err
		}
		if err := coordinator.persistCandidate(ctx, key, state, SQLSinkTwoPhasePrepared); err != nil {
			coordinator.failPrepare(key, state, err)
			return false, err
		}
		coordinator.mu.Lock()
		coordinator.applyEvictionsLocked(state)
		state.durable = true
		state.busy = false
		state.attempt.err = nil
		close(state.attempt.done)
		coordinator.mu.Unlock()
		return true, nil
	}
}

// Commit publishes a previously prepared effect and durably advances its
// progress. A checkpoint failure leaves the prepared state intact so the
// participant can retry the same idempotent Commit operation.
func (coordinator *SQLSinkTwoPhaseCoordinator) Commit(ctx context.Context, commit SQLSinkCommit, participant SQLSinkTwoPhaseParticipant) (bool, error) {
	if coordinator == nil {
		return false, ErrSQLSinkTwoPhaseNil
	}
	if participant == nil {
		return false, ErrSQLSinkTwoPhaseParticipantRequired
	}
	ctx = normalizeSQLSinkTwoPhaseContext(ctx)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key, normalized, err := normalizeSQLSinkTwoPhaseCommit(commit)
	if err != nil {
		return false, err
	}

	for {
		coordinator.mu.Lock()
		coordinator.ensureLocked()
		state, found := coordinator.entries[key]
		if !found {
			coordinator.mu.Unlock()
			return false, ErrSQLSinkTwoPhaseNotPrepared
		}
		if !equalSQLSinkExactlyOnceCommit(state.entry.Commit, normalized) {
			coordinator.mu.Unlock()
			return false, ErrSQLSinkTwoPhaseConflict
		}
		if state.busy {
			attempt := state.attempt
			coordinator.mu.Unlock()
			if err := waitSQLSinkTwoPhaseAttempt(ctx, attempt); err != nil {
				return false, err
			}
			continue
		}
		if state.entry.State == SQLSinkTwoPhaseCommitted {
			coordinator.mu.Unlock()
			return false, nil
		}
		state.busy = true
		state.attempt = &sqlSinkTwoPhaseAttempt{done: make(chan struct{})}
		attempt := state.attempt
		coordinator.mu.Unlock()

		err = invokeSQLSinkTwoPhaseCallback(func() error {
			return participant.Commit(ctx, normalized.IdempotencyKey)
		})
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			coordinator.finishCommitFailure(key, state, attempt, err)
			return false, err
		}
		if err := coordinator.persistCandidate(ctx, key, state, SQLSinkTwoPhaseCommitted); err != nil {
			coordinator.finishCommitFailure(key, state, attempt, err)
			return false, err
		}
		coordinator.mu.Lock()
		if current, exists := coordinator.entries[key]; !exists || current != state {
			coordinator.mu.Unlock()
			return false, ErrSQLSinkTwoPhaseInvalid
		}
		state.entry.State = SQLSinkTwoPhaseCommitted
		state.durable = true
		state.busy = false
		for _, progress := range state.entry.Commit.Progress {
			progressKey := sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}
			coordinator.frontiers[progressKey] = progress.Frontier
			delete(coordinator.inFlightPartitions, progressKey)
		}
		attempt.err = nil
		close(attempt.done)
		coordinator.mu.Unlock()
		return true, nil
	}
}

// Frontier returns the last progress value durably acknowledged for one sink
// partition. Prepared transactions do not affect this value.
func (coordinator *SQLSinkTwoPhaseCoordinator) Frontier(sink, partition string) (uint64, bool) {
	if coordinator == nil {
		return 0, false
	}
	sink = strings.TrimSpace(sink)
	partition = strings.TrimSpace(partition)
	if sink == "" || partition == "" {
		return 0, false
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	frontier, found := coordinator.frontiers[sqlSinkProgressKey{sink: sink, partition: partition}]
	return frontier, found
}

// Snapshot returns a detached checkpoint. A new Prepare callback that has not
// reached durable storage is omitted; an already durable prepared state is
// retained while its Commit callback is running.
func (coordinator *SQLSinkTwoPhaseCoordinator) Snapshot() SQLSinkTwoPhaseCheckpoint {
	if coordinator == nil {
		return SQLSinkTwoPhaseCheckpoint{}
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.snapshotLocked(nil, "")
}

// Restore atomically replaces coordinator state. A failed restore leaves the
// current state unchanged.
func (coordinator *SQLSinkTwoPhaseCoordinator) Restore(snapshot SQLSinkTwoPhaseCheckpoint) error {
	if coordinator == nil {
		return ErrSQLSinkTwoPhaseNil
	}
	normalized, err := normalizeSQLSinkTwoPhaseCheckpoint(snapshot)
	if err != nil {
		return err
	}
	entries := make(map[sqlSinkExactlyOnceKey]*sqlSinkTwoPhaseState, len(normalized.Entries))
	transactions := make(map[sqlSinkExactlyOnceTransactionKey]sqlSinkExactlyOnceKey, len(normalized.Entries))
	order := make([]sqlSinkExactlyOnceKey, 0, len(normalized.Entries))
	inFlight := make(map[sqlSinkProgressKey]struct{})
	for _, entry := range normalized.Entries {
		key, normalizedCommit, err := normalizeSQLSinkTwoPhaseCommit(entry.Commit)
		if err != nil {
			return err
		}
		attempt := &sqlSinkTwoPhaseAttempt{done: make(chan struct{})}
		close(attempt.done)
		entries[key] = &sqlSinkTwoPhaseState{
			entry:   SQLSinkTwoPhaseEntry{Sequence: entry.Sequence, Commit: normalizedCommit, State: entry.State},
			durable: true,
			attempt: attempt,
		}
		transactions[sqlSinkExactlyOnceTransactionKey{sink: normalizedCommit.Sink, transactionID: normalizedCommit.TransactionID}] = key
		order = append(order, key)
		if entry.State == SQLSinkTwoPhasePrepared {
			for _, progress := range normalizedCommit.Progress {
				inFlight[sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}] = struct{}{}
			}
		}
	}
	frontiers := make(map[sqlSinkProgressKey]uint64, len(normalized.Progress))
	for _, progress := range normalized.Progress {
		key, value, err := normalizeSQLSinkProgress(progress)
		if err != nil {
			return err
		}
		frontiers[key] = value.Frontier
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	for _, state := range coordinator.entries {
		if state.busy {
			return ErrSQLSinkTwoPhaseCheckpointInFlight
		}
	}
	coordinator.capacity = normalized.Capacity
	coordinator.nextSequence = normalized.NextSequence
	coordinator.entries = entries
	coordinator.transactions = transactions
	coordinator.order = order
	coordinator.frontiers = frontiers
	coordinator.inFlightPartitions = inFlight
	coordinator.pendingEvictions = make(map[sqlSinkExactlyOnceKey]struct{})
	return nil
}

func (coordinator *SQLSinkTwoPhaseCoordinator) persistCandidate(ctx context.Context, key sqlSinkExactlyOnceKey, state *sqlSinkTwoPhaseState, phase SQLSinkTwoPhaseState) error {
	if coordinator.store == nil {
		return nil
	}
	coordinator.finalizeMu.Lock()
	defer coordinator.finalizeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	coordinator.mu.Lock()
	current, found := coordinator.entries[key]
	if !found || current != state {
		coordinator.mu.Unlock()
		return ErrSQLSinkTwoPhaseInvalid
	}
	snapshot := coordinator.snapshotLocked(state, phase)
	coordinator.mu.Unlock()
	if err := coordinator.store.SaveSQLSinkTwoPhaseCheckpoint(ctx, coordinator.name, snapshot); err != nil {
		return fmt.Errorf("save SQL sink two-phase checkpoint %q: %w", coordinator.name, err)
	}
	return nil
}

func (coordinator *SQLSinkTwoPhaseCoordinator) failPrepare(key sqlSinkExactlyOnceKey, state *sqlSinkTwoPhaseState, err error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	current, found := coordinator.entries[key]
	if !found || current != state {
		return
	}
	coordinator.releaseEvictionsLocked(state)
	coordinator.removeLocked(key, state)
	state.attempt.err = err
	state.busy = false
	close(state.attempt.done)
}

func (coordinator *SQLSinkTwoPhaseCoordinator) finishCommitFailure(key sqlSinkExactlyOnceKey, state *sqlSinkTwoPhaseState, attempt *sqlSinkTwoPhaseAttempt, err error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	current, found := coordinator.entries[key]
	if !found || current != state {
		return
	}
	state.busy = false
	attempt.err = err
	close(attempt.done)
}

func (coordinator *SQLSinkTwoPhaseCoordinator) validateNewCommitLocked(commit SQLSinkCommit) error {
	for _, progress := range commit.Progress {
		progressKey := sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}
		if current, found := coordinator.frontiers[progressKey]; found && progress.Frontier <= current {
			return ErrSQLSinkTwoPhaseStale
		}
		if _, found := coordinator.inFlightPartitions[progressKey]; found {
			return ErrSQLSinkTwoPhaseInFlight
		}
	}
	return nil
}

func (coordinator *SQLSinkTwoPhaseCoordinator) planCapacityLocked() ([]sqlSinkExactlyOnceKey, error) {
	needed := len(coordinator.entries) - coordinator.capacity + 1
	if needed <= 0 {
		return nil, nil
	}
	evictions := make([]sqlSinkExactlyOnceKey, 0, needed)
	for _, key := range coordinator.order {
		state, found := coordinator.entries[key]
		if !found || state.entry.State != SQLSinkTwoPhaseCommitted || state.busy {
			continue
		}
		if _, reserved := coordinator.pendingEvictions[key]; reserved {
			continue
		}
		coordinator.pendingEvictions[key] = struct{}{}
		evictions = append(evictions, key)
		if len(evictions) == needed {
			return evictions, nil
		}
	}
	for _, key := range evictions {
		delete(coordinator.pendingEvictions, key)
	}
	return nil, ErrSQLSinkTwoPhaseCapacity
}

func (coordinator *SQLSinkTwoPhaseCoordinator) applyEvictionsLocked(state *sqlSinkTwoPhaseState) {
	for _, key := range state.evictions {
		delete(coordinator.pendingEvictions, key)
		if evicted, found := coordinator.entries[key]; found && !evicted.busy {
			coordinator.removeLocked(key, evicted)
		}
	}
	state.evictions = nil
}

func (coordinator *SQLSinkTwoPhaseCoordinator) releaseEvictionsLocked(state *sqlSinkTwoPhaseState) {
	for _, key := range state.evictions {
		delete(coordinator.pendingEvictions, key)
	}
	state.evictions = nil
}

func (coordinator *SQLSinkTwoPhaseCoordinator) removeLocked(key sqlSinkExactlyOnceKey, state *sqlSinkTwoPhaseState) {
	delete(coordinator.entries, key)
	delete(coordinator.transactions, sqlSinkExactlyOnceTransactionKey{sink: state.entry.Commit.Sink, transactionID: state.entry.Commit.TransactionID})
	for _, progress := range state.entry.Commit.Progress {
		delete(coordinator.inFlightPartitions, sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition})
	}
	for index, current := range coordinator.order {
		if current == key {
			coordinator.order = append(coordinator.order[:index], coordinator.order[index+1:]...)
			break
		}
	}
}

func (coordinator *SQLSinkTwoPhaseCoordinator) snapshotLocked(candidate *sqlSinkTwoPhaseState, candidatePhase SQLSinkTwoPhaseState) SQLSinkTwoPhaseCheckpoint {
	snapshot := SQLSinkTwoPhaseCheckpoint{
		Capacity:     coordinator.capacity,
		NextSequence: coordinator.nextSequence,
		Entries:      make([]SQLSinkTwoPhaseEntry, 0, len(coordinator.entries)+1),
		Progress:     make([]SQLSinkProgress, 0, len(coordinator.frontiers)+1),
	}
	for _, key := range coordinator.order {
		state, found := coordinator.entries[key]
		if !found || (state.busy && !state.durable && state != candidate) {
			continue
		}
		if candidate != nil && state != candidate && containsSQLSinkTwoPhaseKey(candidate.evictions, key) {
			continue
		}
		entry := state.entry
		if state == candidate && candidatePhase != "" {
			entry.State = candidatePhase
		}
		entry.Commit = cloneSQLSinkCommit(entry.Commit)
		snapshot.Entries = append(snapshot.Entries, entry)
	}
	if candidate != nil {
		found := false
		for _, entry := range snapshot.Entries {
			if entry.Sequence == candidate.entry.Sequence {
				found = true
				break
			}
		}
		if !found {
			entry := candidate.entry
			entry.State = candidatePhase
			entry.Commit = cloneSQLSinkCommit(entry.Commit)
			snapshot.Entries = append(snapshot.Entries, entry)
		}
	}
	frontiers := make(map[sqlSinkProgressKey]uint64, len(coordinator.frontiers)+1)
	for key, frontier := range coordinator.frontiers {
		frontiers[key] = frontier
	}
	if candidate != nil && candidatePhase == SQLSinkTwoPhaseCommitted {
		for _, progress := range candidate.entry.Commit.Progress {
			progressKey := sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}
			if current, found := frontiers[progressKey]; !found || progress.Frontier > current {
				frontiers[progressKey] = progress.Frontier
			}
		}
	}
	for key, frontier := range frontiers {
		snapshot.Progress = append(snapshot.Progress, SQLSinkProgress{Sink: key.sink, Partition: key.partition, Frontier: frontier})
	}
	sort.Slice(snapshot.Entries, func(left, right int) bool {
		return snapshot.Entries[left].Sequence < snapshot.Entries[right].Sequence
	})
	sort.Slice(snapshot.Progress, func(left, right int) bool {
		if snapshot.Progress[left].Sink != snapshot.Progress[right].Sink {
			return snapshot.Progress[left].Sink < snapshot.Progress[right].Sink
		}
		return snapshot.Progress[left].Partition < snapshot.Progress[right].Partition
	})
	return snapshot
}

func (coordinator *SQLSinkTwoPhaseCoordinator) ensureLocked() {
	if coordinator.capacity <= 0 {
		coordinator.capacity = DefaultSQLSinkTwoPhaseCapacity
	}
	if coordinator.nextSequence == 0 {
		coordinator.nextSequence = 1
	}
	if coordinator.entries == nil {
		coordinator.entries = make(map[sqlSinkExactlyOnceKey]*sqlSinkTwoPhaseState)
	}
	if coordinator.transactions == nil {
		coordinator.transactions = make(map[sqlSinkExactlyOnceTransactionKey]sqlSinkExactlyOnceKey)
	}
	if coordinator.frontiers == nil {
		coordinator.frontiers = make(map[sqlSinkProgressKey]uint64)
	}
	if coordinator.inFlightPartitions == nil {
		coordinator.inFlightPartitions = make(map[sqlSinkProgressKey]struct{})
	}
	if coordinator.pendingEvictions == nil {
		coordinator.pendingEvictions = make(map[sqlSinkExactlyOnceKey]struct{})
	}
}

func containsSQLSinkTwoPhaseKey(keys []sqlSinkExactlyOnceKey, target sqlSinkExactlyOnceKey) bool {
	for _, key := range keys {
		if key == target {
			return true
		}
	}
	return false
}

func normalizeSQLSinkTwoPhaseCommit(commit SQLSinkCommit) (sqlSinkExactlyOnceKey, SQLSinkCommit, error) {
	key, normalized, err := normalizeSQLSinkExactlyOnceCommit(commit)
	if err != nil {
		return sqlSinkExactlyOnceKey{}, SQLSinkCommit{}, fmt.Errorf("%w: %v", ErrSQLSinkTwoPhaseInvalid, err)
	}
	return key, normalized, nil
}

func normalizeSQLSinkTwoPhaseCheckpoint(snapshot SQLSinkTwoPhaseCheckpoint) (SQLSinkTwoPhaseCheckpoint, error) {
	capacity := normalizeSQLSinkTwoPhaseCapacity(snapshot.Capacity)
	if len(snapshot.Entries) > capacity || len(snapshot.Progress) > MaxSQLSinkTwoPhasePartitions {
		return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInvalid
	}
	frontiers := make(map[sqlSinkProgressKey]uint64, len(snapshot.Progress))
	explicit := make(map[sqlSinkProgressKey]struct{}, len(snapshot.Progress))
	for _, progress := range snapshot.Progress {
		key, normalized, err := normalizeSQLSinkProgress(progress)
		if err != nil {
			return SQLSinkTwoPhaseCheckpoint{}, fmt.Errorf("%w: %v", ErrSQLSinkTwoPhaseInvalid, err)
		}
		if _, found := frontiers[key]; found {
			return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInvalid
		}
		frontiers[key] = normalized.Frontier
		explicit[key] = struct{}{}
	}
	entries := make([]SQLSinkTwoPhaseEntry, len(snapshot.Entries))
	keys := make(map[sqlSinkExactlyOnceKey]struct{}, len(snapshot.Entries))
	transactions := make(map[sqlSinkExactlyOnceTransactionKey]struct{}, len(snapshot.Entries))
	sequences := make(map[uint64]struct{}, len(snapshot.Entries))
	maxSequence := uint64(0)
	for index, entry := range snapshot.Entries {
		if entry.Sequence == 0 {
			return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInvalid
		}
		if entry.Sequence > maxSequence {
			maxSequence = entry.Sequence
		}
		if _, found := sequences[entry.Sequence]; found {
			return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInvalid
		}
		sequences[entry.Sequence] = struct{}{}
		key, normalized, err := normalizeSQLSinkTwoPhaseCommit(entry.Commit)
		if err != nil {
			return SQLSinkTwoPhaseCheckpoint{}, err
		}
		if entry.State != SQLSinkTwoPhasePrepared && entry.State != SQLSinkTwoPhaseCommitted {
			return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInvalid
		}
		if _, found := keys[key]; found {
			return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInvalid
		}
		transactionKey := sqlSinkExactlyOnceTransactionKey{sink: normalized.Sink, transactionID: normalized.TransactionID}
		if _, found := transactions[transactionKey]; found {
			return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInvalid
		}
		keys[key] = struct{}{}
		transactions[transactionKey] = struct{}{}
		entries[index] = SQLSinkTwoPhaseEntry{Sequence: entry.Sequence, Commit: normalized, State: entry.State}
	}
	for _, entry := range entries {
		if entry.State != SQLSinkTwoPhaseCommitted {
			continue
		}
		for _, progress := range entry.Commit.Progress {
			progressKey := sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}
			if current, found := frontiers[progressKey]; found {
				if _, isExplicit := explicit[progressKey]; isExplicit && progress.Frontier > current {
					return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInvalid
				}
			}
			if current, found := frontiers[progressKey]; !found || progress.Frontier > current {
				frontiers[progressKey] = progress.Frontier
			}
		}
	}
	preparedPartitions := make(map[sqlSinkProgressKey]struct{})
	for _, entry := range entries {
		if entry.State != SQLSinkTwoPhasePrepared {
			continue
		}
		for _, progress := range entry.Commit.Progress {
			progressKey := sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}
			if current, found := frontiers[progressKey]; found && progress.Frontier <= current {
				return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseStale
			}
			if _, found := preparedPartitions[progressKey]; found {
				return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInFlight
			}
			preparedPartitions[progressKey] = struct{}{}
		}
	}
	nextSequence := snapshot.NextSequence
	if nextSequence == 0 {
		if maxSequence == ^uint64(0) {
			return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInvalid
		}
		nextSequence = maxSequence + 1
	}
	if nextSequence <= maxSequence {
		return SQLSinkTwoPhaseCheckpoint{}, ErrSQLSinkTwoPhaseInvalid
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].Sequence < entries[right].Sequence
	})
	normalizedProgress := make([]SQLSinkProgress, 0, len(frontiers))
	for key, frontier := range frontiers {
		normalizedProgress = append(normalizedProgress, SQLSinkProgress{Sink: key.sink, Partition: key.partition, Frontier: frontier})
	}
	sort.Slice(normalizedProgress, func(left, right int) bool {
		if normalizedProgress[left].Sink != normalizedProgress[right].Sink {
			return normalizedProgress[left].Sink < normalizedProgress[right].Sink
		}
		return normalizedProgress[left].Partition < normalizedProgress[right].Partition
	})
	return SQLSinkTwoPhaseCheckpoint{Capacity: capacity, NextSequence: nextSequence, Entries: entries, Progress: normalizedProgress}, nil
}

func normalizeSQLSinkTwoPhaseCapacity(capacity int) int {
	if capacity <= 0 {
		return DefaultSQLSinkTwoPhaseCapacity
	}
	if capacity > MaxSQLSinkTwoPhaseCapacity {
		return MaxSQLSinkTwoPhaseCapacity
	}
	return capacity
}

func validateSQLSinkTwoPhaseName(name string) error {
	if name == "" || len(name) > maxSQLSinkTwoPhaseStringBytes {
		return ErrSQLSinkTwoPhaseInvalid
	}
	return nil
}

func normalizeSQLSinkTwoPhaseContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func waitSQLSinkTwoPhaseAttempt(ctx context.Context, attempt *sqlSinkTwoPhaseAttempt) error {
	if attempt == nil {
		return ErrSQLSinkTwoPhaseInvalid
	}
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func invokeSQLSinkTwoPhaseCallback(callback func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", ErrSQLSinkTwoPhaseParticipantPanic, recovered)
		}
	}()
	return callback()
}
