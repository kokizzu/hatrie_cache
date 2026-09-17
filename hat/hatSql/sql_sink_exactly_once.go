package hatSql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSQLSinkExactlyOnceCapacity bounds retained idempotency records when
	// a caller does not choose a capacity. Acknowledged frontiers are retained
	// independently of this history bound.
	DefaultSQLSinkExactlyOnceCapacity = 1024
	// MaxSQLSinkExactlyOnceCapacity prevents an accidental unbounded replay
	// ledger from consuming process memory.
	MaxSQLSinkExactlyOnceCapacity = 65536
	// MaxSQLSinkExactlyOncePartitions bounds retained sink partition frontiers.
	MaxSQLSinkExactlyOncePartitions      = 65536
	maxSQLSinkExactlyOnceStringBytes     = 1024
	sqlSinkExactlyOnceCheckpointFileMode = 0o600
)

var (
	// ErrSQLSinkExactlyOnceNil reports a method call on a nil ledger.
	ErrSQLSinkExactlyOnceNil = errors.New("SQL sink exactly-once ledger is nil")
	// ErrSQLSinkExactlyOnceInvalid reports malformed commit or checkpoint data.
	ErrSQLSinkExactlyOnceInvalid = errors.New("SQL sink exactly-once data is invalid")
	// ErrSQLSinkExactlyOnceConflict reports reuse of a token or transaction with
	// a different commit definition.
	ErrSQLSinkExactlyOnceConflict = errors.New("SQL sink exactly-once commit conflicts with an existing commit")
	// ErrSQLSinkExactlyOnceStale reports a commit that cannot advance an already
	// acknowledged sink partition frontier.
	ErrSQLSinkExactlyOnceStale = errors.New("SQL sink exactly-once frontier is stale")
	// ErrSQLSinkExactlyOnceInFlight reports a different commit already delivering
	// the same sink partition.
	ErrSQLSinkExactlyOnceInFlight = errors.New("SQL sink exactly-once partition delivery is in flight")
	// ErrSQLSinkExactlyOnceCheckpointInFlight reports restore while a delivery is
	// still executing.
	ErrSQLSinkExactlyOnceCheckpointInFlight = errors.New("SQL sink exactly-once checkpoint restore has an in-flight delivery")
)

// SQLSinkExactlyOnceCheckpoint is the durable state of an exactly-once sink
// ledger. Commits are retained oldest-first up to Capacity; Progress is the
// monotone acknowledged frontier and survives history eviction.
type SQLSinkExactlyOnceCheckpoint struct {
	Capacity int               `json:"capacity"`
	Commits  []SQLSinkCommit   `json:"commits"`
	Progress []SQLSinkProgress `json:"progress"`
}

// SQLSinkExactlyOnceCheckpointStore durably loads and saves one named sink
// ledger. Save must make the checkpoint durable before returning. The external
// sink still needs to honor the idempotency key passed to Commit: a process
// crash after external delivery but before Save can otherwise be retried.
type SQLSinkExactlyOnceCheckpointStore interface {
	LoadSQLSinkExactlyOnceCheckpoint(context.Context, string) (SQLSinkExactlyOnceCheckpoint, bool, error)
	SaveSQLSinkExactlyOnceCheckpoint(context.Context, string, SQLSinkExactlyOnceCheckpoint) error
}

// SQLSinkExactlyOnceOptions configures bounded replay history and optional
// durable recovery. A nil CheckpointStore keeps the ledger entirely in memory.
type SQLSinkExactlyOnceOptions struct {
	Capacity        int
	CheckpointStore SQLSinkExactlyOnceCheckpointStore
	Name            string
}

type sqlSinkExactlyOnceKey struct {
	sink           string
	idempotencyKey string
}

type sqlSinkExactlyOnceTransactionKey struct {
	sink          string
	transactionID string
}

type sqlSinkExactlyOnceState struct {
	commit    SQLSinkCommit
	done      chan struct{}
	committed bool
	err       error
}

// SQLSinkExactlyOnceLedger gates external sink deliveries with a durable
// idempotency key and monotone acknowledged frontiers. It provides exactly-once
// retry decisions; the external sink must apply the supplied key atomically or
// idempotently with its own output.
type SQLSinkExactlyOnceLedger struct {
	mu                 sync.Mutex
	finalizeMu         sync.Mutex
	capacity           int
	commits            map[sqlSinkExactlyOnceKey]*sqlSinkExactlyOnceState
	transactions       map[sqlSinkExactlyOnceTransactionKey]sqlSinkExactlyOnceKey
	order              []sqlSinkExactlyOnceKey
	frontiers          map[sqlSinkProgressKey]uint64
	inFlightPartitions map[sqlSinkProgressKey]struct{}
	store              SQLSinkExactlyOnceCheckpointStore
	name               string
}

// NewSQLSinkExactlyOnceLedger creates a ledger and restores its named
// checkpoint when a store is configured. A missing checkpoint starts empty.
func NewSQLSinkExactlyOnceLedger(ctx context.Context, options SQLSinkExactlyOnceOptions) (*SQLSinkExactlyOnceLedger, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	capacity := normalizeSQLSinkExactlyOnceCapacity(options.Capacity)
	name := strings.TrimSpace(options.Name)
	if options.CheckpointStore != nil {
		var err error
		name, err = normalizeSQLSinkExactlyOnceName(name)
		if err != nil {
			return nil, fmt.Errorf("SQL sink exactly-once checkpoint name is required")
		}
	}
	ledger := &SQLSinkExactlyOnceLedger{
		capacity:           capacity,
		commits:            make(map[sqlSinkExactlyOnceKey]*sqlSinkExactlyOnceState),
		transactions:       make(map[sqlSinkExactlyOnceTransactionKey]sqlSinkExactlyOnceKey),
		frontiers:          make(map[sqlSinkProgressKey]uint64),
		inFlightPartitions: make(map[sqlSinkProgressKey]struct{}),
		store:              options.CheckpointStore,
		name:               name,
	}
	if options.CheckpointStore == nil {
		return ledger, nil
	}
	snapshot, found, err := options.CheckpointStore.LoadSQLSinkExactlyOnceCheckpoint(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("load SQL sink exactly-once checkpoint %q: %w", name, err)
	}
	if !found {
		return ledger, nil
	}
	if err := ledger.Restore(snapshot); err != nil {
		return nil, fmt.Errorf("restore SQL sink exactly-once checkpoint %q: %w", name, err)
	}
	return ledger, nil
}

// NewSQLSinkExactlyOnceLedgerFromSnapshot creates an in-memory ledger from a
// validated checkpoint.
func NewSQLSinkExactlyOnceLedgerFromSnapshot(snapshot SQLSinkExactlyOnceCheckpoint) (*SQLSinkExactlyOnceLedger, error) {
	ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{Capacity: snapshot.Capacity})
	if err != nil {
		return nil, err
	}
	if err := ledger.Restore(snapshot); err != nil {
		return nil, err
	}
	return ledger, nil
}

// Commit invokes apply once for a new idempotency key. The callback receives
// the normalized key and must pass it to the external sink. Duplicate commits
// return false without invoking apply. Callback failures are retryable and do
// not advance the acknowledged frontier.
func (ledger *SQLSinkExactlyOnceLedger) Commit(commit SQLSinkCommit, apply func(string) error) (bool, error) {
	return ledger.CommitContext(context.Background(), commit, apply)
}

// CommitContext is Commit with cancellation-aware checkpoint persistence.
func (ledger *SQLSinkExactlyOnceLedger) CommitContext(ctx context.Context, commit SQLSinkCommit, apply func(string) error) (bool, error) {
	if ledger == nil {
		return false, ErrSQLSinkExactlyOnceNil
	}
	if apply == nil {
		return false, fmt.Errorf("SQL sink exactly-once callback is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key, normalized, err := normalizeSQLSinkExactlyOnceCommit(commit)
	if err != nil {
		return false, err
	}

	ledger.mu.Lock()
	ledger.ensureLocked()
	if existing, found := ledger.commits[key]; found {
		if !equalSQLSinkExactlyOnceCommit(existing.commit, normalized) {
			ledger.mu.Unlock()
			return false, fmt.Errorf("idempotency key %q: %w", key.idempotencyKey, ErrSQLSinkExactlyOnceConflict)
		}
		if existing.committed {
			ledger.mu.Unlock()
			return false, nil
		}
		done := existing.done
		ledger.mu.Unlock()
		select {
		case <-done:
			return false, existing.err
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	transactionKey := sqlSinkExactlyOnceTransactionKey{sink: normalized.Sink, transactionID: normalized.TransactionID}
	if existingKey, found := ledger.transactions[transactionKey]; found && existingKey != key {
		ledger.mu.Unlock()
		return false, fmt.Errorf("transaction %q: %w", normalized.TransactionID, ErrSQLSinkExactlyOnceConflict)
	}
	for _, progress := range normalized.Progress {
		progressKey := sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}
		if current, found := ledger.frontiers[progressKey]; found && progress.Frontier <= current {
			ledger.mu.Unlock()
			return false, fmt.Errorf("sink %q partition %q frontier %d: %w", progress.Sink, progress.Partition, progress.Frontier, ErrSQLSinkExactlyOnceStale)
		}
		if _, found := ledger.inFlightPartitions[progressKey]; found {
			ledger.mu.Unlock()
			return false, fmt.Errorf("sink %q partition %q: %w", progress.Sink, progress.Partition, ErrSQLSinkExactlyOnceInFlight)
		}
	}
	occupiedPartitions := len(ledger.frontiers)
	for _, progress := range normalized.Progress {
		progressKey := sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}
		if _, found := ledger.frontiers[progressKey]; !found {
			occupiedPartitions++
		}
	}
	if occupiedPartitions > MaxSQLSinkExactlyOncePartitions {
		ledger.mu.Unlock()
		return false, ErrSQLSinkExactlyOnceInvalid
	}
	state := &sqlSinkExactlyOnceState{commit: normalized, done: make(chan struct{})}
	ledger.commits[key] = state
	ledger.transactions[transactionKey] = key
	for _, progress := range normalized.Progress {
		ledger.inFlightPartitions[sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}] = struct{}{}
	}
	ledger.mu.Unlock()

	panicked := true
	var panicValue any
	func() {
		defer func() {
			if panicked {
				panicValue = recover()
			}
		}()
		err = apply(normalized.IdempotencyKey)
		panicked = false
	}()
	if panicked {
		panicErr := fmt.Errorf("SQL sink exactly-once callback panicked: %v", panicValue)
		ledger.abort(key, state, panicErr)
		panic(panicValue)
	}
	if err != nil {
		ledger.abort(key, state, err)
		return false, err
	}
	if err := ctx.Err(); err != nil {
		ledger.abort(key, state, err)
		return false, err
	}
	return ledger.finish(ctx, key, state)
}

// Committed reports whether an idempotency key is retained as committed.
func (ledger *SQLSinkExactlyOnceLedger) Committed(sink, idempotencyKey string) bool {
	if ledger == nil {
		return false
	}
	sink = strings.TrimSpace(sink)
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if sink == "" || idempotencyKey == "" {
		return false
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	state, found := ledger.commits[sqlSinkExactlyOnceKey{sink: sink, idempotencyKey: idempotencyKey}]
	return found && state.committed
}

// Frontier returns the durable acknowledged frontier for one sink partition.
func (ledger *SQLSinkExactlyOnceLedger) Frontier(sink, partition string) (uint64, bool) {
	if ledger == nil {
		return 0, false
	}
	sink = strings.TrimSpace(sink)
	partition = strings.TrimSpace(partition)
	if sink == "" || partition == "" {
		return 0, false
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	frontier, found := ledger.frontiers[sqlSinkProgressKey{sink: sink, partition: partition}]
	return frontier, found
}

// Snapshot returns an independently owned checkpoint. Commits are ordered
// oldest-first and retained history is bounded by the configured capacity.
func (ledger *SQLSinkExactlyOnceLedger) Snapshot() SQLSinkExactlyOnceCheckpoint {
	if ledger == nil {
		return SQLSinkExactlyOnceCheckpoint{}
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return cloneSQLSinkExactlyOnceCheckpoint(ledger.snapshotLocked(nil))
}

// Restore atomically replaces committed history and acknowledged frontiers.
// Invalid data or a restore during delivery leaves the current state unchanged.
func (ledger *SQLSinkExactlyOnceLedger) Restore(snapshot SQLSinkExactlyOnceCheckpoint) error {
	if ledger == nil {
		return ErrSQLSinkExactlyOnceNil
	}
	normalized, err := normalizeSQLSinkExactlyOnceCheckpoint(snapshot)
	if err != nil {
		return err
	}
	commits := make(map[sqlSinkExactlyOnceKey]*sqlSinkExactlyOnceState, len(normalized.Commits))
	transactions := make(map[sqlSinkExactlyOnceTransactionKey]sqlSinkExactlyOnceKey, len(normalized.Commits))
	order := make([]sqlSinkExactlyOnceKey, 0, len(normalized.Commits))
	for _, commit := range normalized.Commits {
		key, normalizedCommit, err := normalizeSQLSinkExactlyOnceCommit(commit)
		if err != nil {
			return err
		}
		state := &sqlSinkExactlyOnceState{commit: normalizedCommit, done: closedSQLSinkExactlyOnceChannel(), committed: true}
		commits[key] = state
		transactions[sqlSinkExactlyOnceTransactionKey{sink: normalizedCommit.Sink, transactionID: normalizedCommit.TransactionID}] = key
		order = append(order, key)
	}
	frontiers := make(map[sqlSinkProgressKey]uint64, len(normalized.Progress))
	for _, progress := range normalized.Progress {
		key, value, err := normalizeSQLSinkProgress(progress)
		if err != nil {
			return err
		}
		frontiers[key] = value.Frontier
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	for _, state := range ledger.commits {
		if !state.committed {
			return ErrSQLSinkExactlyOnceCheckpointInFlight
		}
	}
	ledger.capacity = normalized.Capacity
	ledger.commits = commits
	ledger.transactions = transactions
	ledger.order = order
	ledger.frontiers = frontiers
	ledger.inFlightPartitions = make(map[sqlSinkProgressKey]struct{})
	return nil
}

func (ledger *SQLSinkExactlyOnceLedger) finish(ctx context.Context, key sqlSinkExactlyOnceKey, state *sqlSinkExactlyOnceState) (bool, error) {
	if ledger.store == nil {
		ledger.mu.Lock()
		defer ledger.mu.Unlock()
		ledger.commitLocked(key, state)
		return true, nil
	}
	ledger.finalizeMu.Lock()
	defer ledger.finalizeMu.Unlock()
	ledger.mu.Lock()
	snapshot := ledger.snapshotLocked(state)
	ledger.mu.Unlock()
	if err := ctx.Err(); err != nil {
		ledger.abort(key, state, err)
		return false, err
	}
	if err := ledger.store.SaveSQLSinkExactlyOnceCheckpoint(ctx, ledger.name, snapshot); err != nil {
		ledger.abort(key, state, fmt.Errorf("save SQL sink exactly-once checkpoint %q: %w", ledger.name, err))
		return false, fmt.Errorf("save SQL sink exactly-once checkpoint %q: %w", ledger.name, err)
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.commitLocked(key, state)
	return true, nil
}

func (ledger *SQLSinkExactlyOnceLedger) commitLocked(key sqlSinkExactlyOnceKey, state *sqlSinkExactlyOnceState) {
	if current, found := ledger.commits[key]; !found || current != state {
		return
	}
	state.committed = true
	ledger.order = append(ledger.order, key)
	for _, progress := range state.commit.Progress {
		progressKey := sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}
		ledger.frontiers[progressKey] = progress.Frontier
		delete(ledger.inFlightPartitions, progressKey)
	}
	close(state.done)
	for len(ledger.order) > ledger.capacity {
		evicted := ledger.order[0]
		ledger.order = ledger.order[1:]
		if evicted == key {
			continue
		}
		if existing, found := ledger.commits[evicted]; found && existing.committed {
			delete(ledger.commits, evicted)
			delete(ledger.transactions, sqlSinkExactlyOnceTransactionKey{sink: existing.commit.Sink, transactionID: existing.commit.TransactionID})
		}
	}
}

func (ledger *SQLSinkExactlyOnceLedger) abort(key sqlSinkExactlyOnceKey, state *sqlSinkExactlyOnceState, err error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	current, found := ledger.commits[key]
	if !found || current != state {
		return
	}
	state.err = err
	delete(ledger.commits, key)
	delete(ledger.transactions, sqlSinkExactlyOnceTransactionKey{sink: state.commit.Sink, transactionID: state.commit.TransactionID})
	for _, progress := range state.commit.Progress {
		delete(ledger.inFlightPartitions, sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition})
	}
	close(state.done)
}

func (ledger *SQLSinkExactlyOnceLedger) snapshotLocked(pending *sqlSinkExactlyOnceState) SQLSinkExactlyOnceCheckpoint {
	snapshot := SQLSinkExactlyOnceCheckpoint{
		Capacity: ledger.capacity,
		Commits:  make([]SQLSinkCommit, 0, len(ledger.order)+1),
		Progress: make([]SQLSinkProgress, 0, len(ledger.frontiers)+1),
	}
	for _, key := range ledger.order {
		if state, found := ledger.commits[key]; found && state.committed {
			snapshot.Commits = append(snapshot.Commits, state.commit)
		}
	}
	frontiers := make(map[sqlSinkProgressKey]uint64, len(ledger.frontiers)+1)
	for key, frontier := range ledger.frontiers {
		frontiers[key] = frontier
	}
	if pending != nil {
		snapshot.Commits = append(snapshot.Commits, pending.commit)
		for _, progress := range pending.commit.Progress {
			progressKey := sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}
			if current, found := frontiers[progressKey]; !found || progress.Frontier > current {
				frontiers[progressKey] = progress.Frontier
			}
		}
	}
	if pending != nil && len(snapshot.Commits) > ledger.capacity {
		snapshot.Commits = snapshot.Commits[len(snapshot.Commits)-ledger.capacity:]
	}
	for key, frontier := range frontiers {
		snapshot.Progress = append(snapshot.Progress, SQLSinkProgress{Sink: key.sink, Partition: key.partition, Frontier: frontier})
	}
	sort.Slice(snapshot.Progress, func(left, right int) bool {
		if snapshot.Progress[left].Sink != snapshot.Progress[right].Sink {
			return snapshot.Progress[left].Sink < snapshot.Progress[right].Sink
		}
		return snapshot.Progress[left].Partition < snapshot.Progress[right].Partition
	})
	return snapshot
}

func (ledger *SQLSinkExactlyOnceLedger) ensureLocked() {
	if ledger.capacity <= 0 {
		ledger.capacity = DefaultSQLSinkExactlyOnceCapacity
	}
	if ledger.commits == nil {
		ledger.commits = make(map[sqlSinkExactlyOnceKey]*sqlSinkExactlyOnceState)
	}
	if ledger.transactions == nil {
		ledger.transactions = make(map[sqlSinkExactlyOnceTransactionKey]sqlSinkExactlyOnceKey)
	}
	if ledger.frontiers == nil {
		ledger.frontiers = make(map[sqlSinkProgressKey]uint64)
	}
	if ledger.inFlightPartitions == nil {
		ledger.inFlightPartitions = make(map[sqlSinkProgressKey]struct{})
	}
}

// FileSQLSinkExactlyOnceCheckpointStore persists named ledgers in a private
// versioned binary file using same-directory atomic replacement. It accepts
// legacy JSON on read and rejects symlink, non-regular, and group/world-readable
// checkpoint files.
type FileSQLSinkExactlyOnceCheckpointStore struct {
	mu   sync.Mutex
	path string
}

// NewFileSQLSinkExactlyOnceCheckpointStore creates a file-backed checkpoint
// store. The parent directory must already exist when saving for the first
// time.
func NewFileSQLSinkExactlyOnceCheckpointStore(path string) (*FileSQLSinkExactlyOnceCheckpointStore, error) {
	path = strings.TrimSpace(path)
	if path == "" || path == "." {
		return nil, fmt.Errorf("SQL sink exactly-once checkpoint path is required")
	}
	return &FileSQLSinkExactlyOnceCheckpointStore{path: filepath.Clean(path)}, nil
}

// LoadSQLSinkExactlyOnceCheckpoint loads one named checkpoint. A missing file
// or name is an empty store.
func (store *FileSQLSinkExactlyOnceCheckpointStore) LoadSQLSinkExactlyOnceCheckpoint(ctx context.Context, name string) (SQLSinkExactlyOnceCheckpoint, bool, error) {
	if store == nil {
		return SQLSinkExactlyOnceCheckpoint{}, false, fmt.Errorf("SQL sink exactly-once checkpoint store is nil")
	}
	if err := sqlSinkExactlyOnceContextError(ctx); err != nil {
		return SQLSinkExactlyOnceCheckpoint{}, false, err
	}
	name, err := normalizeSQLSinkExactlyOnceName(name)
	if err != nil {
		return SQLSinkExactlyOnceCheckpoint{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	checkpoints, found, err := store.loadLocked()
	if err != nil || !found {
		return SQLSinkExactlyOnceCheckpoint{}, false, err
	}
	snapshot, found := checkpoints[name]
	if !found {
		return SQLSinkExactlyOnceCheckpoint{}, false, nil
	}
	validated, err := normalizeSQLSinkExactlyOnceCheckpoint(snapshot)
	if err != nil {
		return SQLSinkExactlyOnceCheckpoint{}, false, err
	}
	return cloneSQLSinkExactlyOnceCheckpoint(validated), true, nil
}

// SaveSQLSinkExactlyOnceCheckpoint atomically replaces one named checkpoint.
func (store *FileSQLSinkExactlyOnceCheckpointStore) SaveSQLSinkExactlyOnceCheckpoint(ctx context.Context, name string, snapshot SQLSinkExactlyOnceCheckpoint) error {
	if store == nil {
		return fmt.Errorf("SQL sink exactly-once checkpoint store is nil")
	}
	if err := sqlSinkExactlyOnceContextError(ctx); err != nil {
		return err
	}
	name, err := normalizeSQLSinkExactlyOnceName(name)
	if err != nil {
		return err
	}
	if err := validateSQLSinkExactlyOnceCheckpoint(snapshot); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	checkpoints, _, err := store.loadLocked()
	if err != nil {
		return err
	}
	if checkpoints == nil {
		checkpoints = make(map[string]SQLSinkExactlyOnceCheckpoint)
	}
	checkpoints[name] = snapshot
	data, err := marshalSQLSinkExactlyOnceFile(sqlSinkExactlyOnceFile{Checkpoints: checkpoints})
	if err != nil {
		return fmt.Errorf("marshal SQL sink exactly-once checkpoints: %w", err)
	}
	if err := sqlSinkExactlyOnceContextError(ctx); err != nil {
		return err
	}
	return store.writeLocked(data)
}

type sqlSinkExactlyOnceFile struct {
	Checkpoints map[string]SQLSinkExactlyOnceCheckpoint `json:"checkpoints"`
}

func (store *FileSQLSinkExactlyOnceCheckpointStore) loadLocked() (map[string]SQLSinkExactlyOnceCheckpoint, bool, error) {
	info, err := os.Lstat(store.path)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("inspect SQL sink exactly-once checkpoint file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, false, fmt.Errorf("SQL sink exactly-once checkpoint path must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, false, fmt.Errorf("SQL sink exactly-once checkpoint file permissions must not grant group or other access")
	}
	data, err := os.ReadFile(store.path)
	if err != nil {
		return nil, false, fmt.Errorf("read SQL sink exactly-once checkpoint file: %w", err)
	}
	file := sqlSinkExactlyOnceFile{}
	if len(data) >= 5 && data[0] == sqlSinkExactlyOnceFileMagic0 && data[1] == sqlSinkExactlyOnceFileMagic1 && data[2] == sqlSinkExactlyOnceFileMagic2 && data[3] == sqlSinkExactlyOnceFileMagic3 {
		file, err = unmarshalSQLSinkExactlyOnceFile(data)
		if err != nil {
			return nil, false, fmt.Errorf("decode SQL sink exactly-once checkpoint file: %w", err)
		}
	} else if err := json.Unmarshal(data, &file); err != nil {
		return nil, false, fmt.Errorf("decode SQL sink exactly-once checkpoint file: %w", err)
	}
	return file.Checkpoints, true, nil
}

func (store *FileSQLSinkExactlyOnceCheckpointStore) writeLocked(data []byte) error {
	directory := filepath.Dir(store.path)
	info, err := os.Stat(directory)
	if err != nil {
		return fmt.Errorf("inspect SQL sink exactly-once checkpoint directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("SQL sink exactly-once checkpoint parent is not a directory")
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary SQL sink exactly-once checkpoint file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(sqlSinkExactlyOnceCheckpointFileMode); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set SQL sink exactly-once checkpoint permissions: %w", err)
	}
	if written, err := temporary.Write(data); err != nil || written != len(data) {
		_ = temporary.Close()
		if err == nil {
			err = io.ErrShortWrite
		}
		return fmt.Errorf("write SQL sink exactly-once checkpoint file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync SQL sink exactly-once checkpoint file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close SQL sink exactly-once checkpoint file: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("replace SQL sink exactly-once checkpoint file: %w", err)
	}
	removeTemporary = false
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open SQL sink exactly-once checkpoint directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync SQL sink exactly-once checkpoint directory: %w", err)
	}
	return nil
}

func normalizeSQLSinkExactlyOnceCommit(commit SQLSinkCommit) (sqlSinkExactlyOnceKey, SQLSinkCommit, error) {
	_, normalized, err := normalizeSQLSinkCommit(commit)
	if err != nil {
		return sqlSinkExactlyOnceKey{}, SQLSinkCommit{}, fmt.Errorf("%w: %v", ErrSQLSinkExactlyOnceInvalid, err)
	}
	normalized.IdempotencyKey = strings.TrimSpace(normalized.IdempotencyKey)
	if normalized.IdempotencyKey == "" {
		normalized.IdempotencyKey = normalized.TransactionID
	}
	if len(normalized.TransactionID) > maxSQLSinkExactlyOnceStringBytes || len(normalized.IdempotencyKey) > maxSQLSinkExactlyOnceStringBytes || len(normalized.Progress) > maxSQLSinkExactlyOnceProgress {
		return sqlSinkExactlyOnceKey{}, SQLSinkCommit{}, ErrSQLSinkExactlyOnceInvalid
	}
	return sqlSinkExactlyOnceKey{sink: normalized.Sink, idempotencyKey: normalized.IdempotencyKey}, normalized, nil
}

func normalizeSQLSinkExactlyOnceCheckpoint(snapshot SQLSinkExactlyOnceCheckpoint) (SQLSinkExactlyOnceCheckpoint, error) {
	capacity := snapshot.Capacity
	if capacity <= 0 {
		capacity = DefaultSQLSinkExactlyOnceCapacity
	}
	if capacity > MaxSQLSinkExactlyOnceCapacity || len(snapshot.Commits) > capacity || len(snapshot.Progress) > MaxSQLSinkExactlyOncePartitions {
		return SQLSinkExactlyOnceCheckpoint{}, ErrSQLSinkExactlyOnceInvalid
	}
	frontiers := make(map[sqlSinkProgressKey]uint64, len(snapshot.Progress))
	explicitFrontiers := make(map[sqlSinkProgressKey]struct{}, len(snapshot.Progress))
	for _, progress := range snapshot.Progress {
		key, normalized, err := normalizeSQLSinkProgress(progress)
		if err != nil {
			return SQLSinkExactlyOnceCheckpoint{}, fmt.Errorf("%w: %v", ErrSQLSinkExactlyOnceInvalid, err)
		}
		if _, found := frontiers[key]; found {
			return SQLSinkExactlyOnceCheckpoint{}, ErrSQLSinkExactlyOnceInvalid
		}
		frontiers[key] = normalized.Frontier
		explicitFrontiers[key] = struct{}{}
	}
	commits := make([]SQLSinkCommit, len(snapshot.Commits))
	keys := make(map[sqlSinkExactlyOnceKey]struct{}, len(snapshot.Commits))
	transactions := make(map[sqlSinkExactlyOnceTransactionKey]struct{}, len(snapshot.Commits))
	for index, commit := range snapshot.Commits {
		key, normalized, err := normalizeSQLSinkExactlyOnceCommit(commit)
		if err != nil {
			return SQLSinkExactlyOnceCheckpoint{}, err
		}
		if _, found := keys[key]; found {
			return SQLSinkExactlyOnceCheckpoint{}, ErrSQLSinkExactlyOnceInvalid
		}
		transactionKey := sqlSinkExactlyOnceTransactionKey{sink: normalized.Sink, transactionID: normalized.TransactionID}
		if _, found := transactions[transactionKey]; found {
			return SQLSinkExactlyOnceCheckpoint{}, ErrSQLSinkExactlyOnceInvalid
		}
		keys[key] = struct{}{}
		transactions[transactionKey] = struct{}{}
		commits[index] = normalized
		for _, progress := range normalized.Progress {
			progressKey := sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}
			if current, found := frontiers[progressKey]; found {
				if _, explicit := explicitFrontiers[progressKey]; explicit && progress.Frontier > current {
					return SQLSinkExactlyOnceCheckpoint{}, ErrSQLSinkExactlyOnceInvalid
				}
			}
			if current, found := frontiers[progressKey]; !found || progress.Frontier > current {
				frontiers[progressKey] = progress.Frontier
			}
		}
	}
	progress := make([]SQLSinkProgress, 0, len(frontiers))
	for key, frontier := range frontiers {
		progress = append(progress, SQLSinkProgress{Sink: key.sink, Partition: key.partition, Frontier: frontier})
	}
	sort.Slice(progress, func(left, right int) bool {
		if progress[left].Sink != progress[right].Sink {
			return progress[left].Sink < progress[right].Sink
		}
		return progress[left].Partition < progress[right].Partition
	})
	return SQLSinkExactlyOnceCheckpoint{Capacity: capacity, Commits: commits, Progress: progress}, nil
}

func validateSQLSinkExactlyOnceCheckpoint(snapshot SQLSinkExactlyOnceCheckpoint) error {
	capacity := snapshot.Capacity
	if capacity <= 0 {
		capacity = DefaultSQLSinkExactlyOnceCapacity
	}
	if capacity > MaxSQLSinkExactlyOnceCapacity || len(snapshot.Commits) > capacity || len(snapshot.Progress) > MaxSQLSinkExactlyOncePartitions {
		return ErrSQLSinkExactlyOnceInvalid
	}
	frontiers := make(map[sqlSinkProgressKey]uint64, len(snapshot.Progress))
	for _, progress := range snapshot.Progress {
		key, normalized, err := normalizeSQLSinkProgress(progress)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrSQLSinkExactlyOnceInvalid, err)
		}
		if _, found := frontiers[key]; found {
			return ErrSQLSinkExactlyOnceInvalid
		}
		frontiers[key] = normalized.Frontier
	}
	keys := make(map[sqlSinkExactlyOnceKey]struct{}, len(snapshot.Commits))
	transactions := make(map[sqlSinkExactlyOnceTransactionKey]struct{}, len(snapshot.Commits))
	for _, commit := range snapshot.Commits {
		sink := strings.TrimSpace(commit.Sink)
		transactionID := strings.TrimSpace(commit.TransactionID)
		idempotencyKey := strings.TrimSpace(commit.IdempotencyKey)
		if idempotencyKey == "" {
			idempotencyKey = transactionID
		}
		if sink == "" || transactionID == "" || idempotencyKey == "" || len(transactionID) > maxSQLSinkExactlyOnceStringBytes || len(idempotencyKey) > maxSQLSinkExactlyOnceStringBytes || len(commit.Progress) == 0 || len(commit.Progress) > maxSQLSinkExactlyOnceProgress {
			return ErrSQLSinkExactlyOnceInvalid
		}
		key := sqlSinkExactlyOnceKey{sink: sink, idempotencyKey: idempotencyKey}
		if _, found := keys[key]; found {
			return ErrSQLSinkExactlyOnceInvalid
		}
		transactionKey := sqlSinkExactlyOnceTransactionKey{sink: sink, transactionID: transactionID}
		if _, found := transactions[transactionKey]; found {
			return ErrSQLSinkExactlyOnceInvalid
		}
		keys[key] = struct{}{}
		transactions[transactionKey] = struct{}{}
		seenProgress := make(map[sqlSinkProgressKey]struct{}, len(commit.Progress))
		for _, progress := range commit.Progress {
			progressKey, normalized, err := normalizeSQLSinkProgress(progress)
			if err != nil || normalized.Sink != sink {
				return ErrSQLSinkExactlyOnceInvalid
			}
			if _, found := seenProgress[progressKey]; found {
				return ErrSQLSinkExactlyOnceInvalid
			}
			seenProgress[progressKey] = struct{}{}
			if current, found := frontiers[progressKey]; found {
				if normalized.Frontier > current {
					return ErrSQLSinkExactlyOnceInvalid
				}
				continue
			}
			frontiers[progressKey] = normalized.Frontier
		}
	}
	return nil
}

func normalizeSQLSinkExactlyOnceCapacity(capacity int) int {
	if capacity <= 0 {
		return DefaultSQLSinkExactlyOnceCapacity
	}
	if capacity > MaxSQLSinkExactlyOnceCapacity {
		return MaxSQLSinkExactlyOnceCapacity
	}
	return capacity
}

func normalizeSQLSinkExactlyOnceName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxSQLSinkExactlyOnceStringBytes {
		return "", ErrSQLSinkExactlyOnceInvalid
	}
	return name, nil
}

func equalSQLSinkExactlyOnceCommit(left, right SQLSinkCommit) bool {
	return left.Sink == right.Sink && left.TransactionID == right.TransactionID && left.IdempotencyKey == right.IdempotencyKey && equalSQLSinkProgress(left.Progress, right.Progress)
}

func cloneSQLSinkCommit(commit SQLSinkCommit) SQLSinkCommit {
	commit.Progress = cloneSQLSinkProgress(commit.Progress)
	return commit
}

func cloneSQLSinkExactlyOnceCheckpoint(snapshot SQLSinkExactlyOnceCheckpoint) SQLSinkExactlyOnceCheckpoint {
	commits := snapshot.Commits
	snapshot.Commits = make([]SQLSinkCommit, len(commits))
	for index, commit := range commits {
		snapshot.Commits[index] = cloneSQLSinkCommit(commit)
	}
	snapshot.Progress = cloneSQLSinkProgress(snapshot.Progress)
	return snapshot
}

func closedSQLSinkExactlyOnceChannel() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func sqlSinkExactlyOnceContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("SQL sink exactly-once checkpoint context: %w", err)
	}
	return nil
}
