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
	// DefaultSQLMultiSourceSnapshotMaxSources bounds one coordinated snapshot.
	DefaultSQLMultiSourceSnapshotMaxSources = 16
	// MaxSQLMultiSourceSnapshotMaxSources prevents accidental unbounded fan-out.
	MaxSQLMultiSourceSnapshotMaxSources = 256
)

var (
	ErrSQLMultiSourceSnapshotNil             = errors.New("hatSql: multi-source snapshot coordinator is nil")
	ErrSQLMultiSourceSnapshotOptionsInvalid  = errors.New("hatSql: multi-source snapshot options are invalid")
	ErrSQLMultiSourceSnapshotRequestInvalid  = errors.New("hatSql: multi-source snapshot request is invalid")
	ErrSQLMultiSourceSnapshotProvider        = errors.New("hatSql: multi-source snapshot provider is required")
	ErrSQLMultiSourceSnapshotCheckpoint      = errors.New("hatSql: multi-source snapshot checkpoint store is required")
	ErrSQLMultiSourceSnapshotLoad            = errors.New("hatSql: multi-source snapshot checkpoint load failed")
	ErrSQLMultiSourceSnapshotCommit          = errors.New("hatSql: multi-source snapshot checkpoint commit failed")
	ErrSQLMultiSourceSnapshotDuplicateSource = errors.New("hatSql: multi-source snapshot source is duplicated")
	ErrSQLMultiSourceSnapshotSourceLimit     = errors.New("hatSql: multi-source snapshot source limit exceeded")
	ErrSQLMultiSourceSnapshotIdentity        = errors.New("hatSql: multi-source snapshot identity is invalid")
	ErrSQLMultiSourceSnapshotUnavailable    = errors.New("hatSql: multi-source snapshot is not published")
	ErrSQLMultiSourceSnapshotInvalid         = errors.New("hatSql: multi-source snapshot is invalid")

	// These aliases let callers handle a coordinated source failure with the
	// same errors.Is checks used by single-source ingestion.
	ErrSQLMultiSourceSnapshotAuthentication = ErrSQLExternalSnapshotAuthentication
)

// SQLMultiSourceSnapshotCoordinatorOptions bounds one coordinated snapshot.
// Zero values select conservative defaults for all bounds.
type SQLMultiSourceSnapshotCoordinatorOptions struct {
	MaxSources          int
	MaxRowsPerSource    int
	MaxOffsetsPerSource int
	MaxPageRows         int
}

// SQLMultiSourceSnapshotCaptureOptions controls one coordinated capture.
type SQLMultiSourceSnapshotCaptureOptions struct {
	RequireSnapshotIDs bool
}

// SQLMultiSourceSnapshotRequest describes one authenticated source capture.
// Source IDs are unique within one coordinated snapshot.
type SQLMultiSourceSnapshotRequest struct {
	Source   string
	Key      string
	Kind     string
	Provider SQLExternalSnapshotProvider
}

// SQLMultiSourceSnapshot retains the exact source snapshots that form one
// atomic initial view. Generation and each source's SnapshotID/Offsets are
// retained so live tails can start after the captured point.
type SQLMultiSourceSnapshot struct {
	Generation uint64
	Sources    []SQLExternalSnapshot
}

// SQLMultiSourceSnapshotCheckpointStore atomically persists all sources in one
// commit. A found snapshot must contain a complete coordinated view.
type SQLMultiSourceSnapshotCheckpointStore interface {
	Load(context.Context) (SQLMultiSourceSnapshot, bool, error)
	Commit(context.Context, SQLMultiSourceSnapshot) error
}

// SQLMultiSourceSnapshotResult reports an atomic capture or recovery.
type SQLMultiSourceSnapshotResult struct {
	Snapshot     SQLMultiSourceSnapshot
	View         *SQLMultiSourceSnapshotView
	Generation   uint64
	Restored     bool
	Checkpointed bool
}

// SQLMultiSourceSnapshotView is an immutable, detached view of one
// coordinated snapshot. ResolveSQLSource returns cloned rows.
type SQLMultiSourceSnapshotView struct {
	snapshot SQLMultiSourceSnapshot
	indexes  map[sqlMultiSourceSnapshotLookupKey]int
}

type sqlMultiSourceSnapshotLookupKey struct {
	kind string
	key  string
}

// ResolveSQLSource returns rows for a source kind and key from this view.
func (view *SQLMultiSourceSnapshotView) ResolveSQLSource(kind, key string) ([]Row, error) {
	if view == nil {
		return nil, ErrSQLMultiSourceSnapshotNil
	}
	normalizedKind := normalizeSQLMultiSourceSnapshotKind(kind)
	normalizedKey := strings.TrimSpace(key)
	if normalizedKind == "" || normalizedKey == "" {
		return nil, ErrSQLMultiSourceSnapshotIdentity
	}
	index, ok := view.indexes[sqlMultiSourceSnapshotLookupKey{kind: normalizedKind, key: normalizedKey}]
	if !ok {
		return nil, ErrSQLMultiSourceSnapshotIdentity
	}
	return cloneSQLExternalSnapshotRows(view.snapshot.Sources[index].Rows), nil
}

// Sources returns detached source snapshots in deterministic order.
func (view *SQLMultiSourceSnapshotView) Sources() []SQLExternalSnapshot {
	if view == nil {
		return nil
	}
	return cloneSQLMultiSourceSnapshot(view.snapshot).Sources
}

// Generation returns the view's publication generation.
func (view *SQLMultiSourceSnapshotView) Generation() uint64 {
	if view == nil {
		return 0
	}
	return view.snapshot.Generation
}

// SQLMultiSourceSnapshotCoordinator captures multiple source snapshots and
// publishes them as one read-consistent view. The capture mutex serializes
// bootstrap attempts while the read/write mutex keeps publication atomic.
type SQLMultiSourceSnapshotCoordinator struct {
	mu          sync.RWMutex
	captureMu   sync.Mutex
	maxSources  int
	maxRows     int
	maxOffsets  int
	maxPageRows int
	snapshot    SQLMultiSourceSnapshot
	view        *SQLMultiSourceSnapshotView
	generation  uint64
}

type sqlMultiSourceSnapshotCaptureResult struct {
	snapshot SQLExternalSnapshot
	err      error
}

// NewSQLMultiSourceSnapshotCoordinator creates a bounded coordinator.
func NewSQLMultiSourceSnapshotCoordinator(options SQLMultiSourceSnapshotCoordinatorOptions) (*SQLMultiSourceSnapshotCoordinator, error) {
	if options.MaxSources < 0 || options.MaxRowsPerSource < 0 || options.MaxOffsetsPerSource < 0 || options.MaxPageRows < 0 {
		return nil, ErrSQLMultiSourceSnapshotOptionsInvalid
	}
	maxSources := options.MaxSources
	if maxSources == 0 {
		maxSources = DefaultSQLMultiSourceSnapshotMaxSources
	}
	if maxSources > MaxSQLMultiSourceSnapshotMaxSources {
		return nil, ErrSQLMultiSourceSnapshotOptionsInvalid
	}
	maxRows, maxOffsets, maxPageRows := options.MaxRowsPerSource, options.MaxOffsetsPerSource, options.MaxPageRows
	if maxRows == 0 {
		maxRows = DefaultSQLExternalSnapshotMaxRows
	}
	if maxOffsets == 0 {
		maxOffsets = DefaultSQLExternalSnapshotMaxOffsets
	}
	if maxPageRows == 0 {
		maxPageRows = DefaultSQLExternalSnapshotMaxPageRows
	}
	maxRows, maxOffsets, maxPageRows, err := normalizeSQLExternalSnapshotLimits(maxRows, maxOffsets, maxPageRows)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSQLMultiSourceSnapshotOptionsInvalid, err)
	}
	return &SQLMultiSourceSnapshotCoordinator{
		maxSources:  maxSources,
		maxRows:     maxRows,
		maxOffsets:  maxOffsets,
		maxPageRows: maxPageRows,
	}, nil
}

// CaptureWithCheckpoint captures every requested source concurrently, then
// commits and publishes one complete snapshot. A stored snapshot is restored
// without contacting providers. Any source or commit failure leaves the prior
// published view unchanged.
func (coordinator *SQLMultiSourceSnapshotCoordinator) CaptureWithCheckpoint(ctx context.Context, requests []SQLMultiSourceSnapshotRequest, store SQLMultiSourceSnapshotCheckpointStore, options SQLMultiSourceSnapshotCaptureOptions) (SQLMultiSourceSnapshotResult, error) {
	if coordinator == nil {
		return SQLMultiSourceSnapshotResult{}, ErrSQLMultiSourceSnapshotNil
	}
	if store == nil {
		return SQLMultiSourceSnapshotResult{}, ErrSQLMultiSourceSnapshotCheckpoint
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SQLMultiSourceSnapshotResult{}, err
	}
	normalizedRequests, err := coordinator.normalizeRequests(requests)
	if err != nil {
		return SQLMultiSourceSnapshotResult{}, err
	}
	coordinator.captureMu.Lock()
	defer coordinator.captureMu.Unlock()

	stored, found, err := store.Load(ctx)
	if err != nil {
		return SQLMultiSourceSnapshotResult{}, fmt.Errorf("%w: %v", ErrSQLMultiSourceSnapshotLoad, err)
	}
	if found {
		snapshot, err := coordinator.normalizeStoredSnapshot(stored, options.RequireSnapshotIDs)
		if err != nil {
			return SQLMultiSourceSnapshotResult{}, err
		}
		view := newSQLMultiSourceSnapshotView(snapshot)
		coordinator.mu.Lock()
		coordinator.snapshot = cloneSQLMultiSourceSnapshot(snapshot)
		coordinator.generation = snapshot.Generation
		coordinator.view = view
		coordinator.mu.Unlock()
		return SQLMultiSourceSnapshotResult{
			Snapshot:   cloneSQLMultiSourceSnapshot(snapshot),
			View:       view,
			Generation: snapshot.Generation,
			Restored:   true,
		}, nil
	}

	for _, request := range normalizedRequests {
		if request.Provider == nil {
			return SQLMultiSourceSnapshotResult{}, ErrSQLMultiSourceSnapshotProvider
		}
	}
	captureContext, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]sqlMultiSourceSnapshotCaptureResult, len(normalizedRequests))
	var waitGroup sync.WaitGroup
	for index, request := range normalizedRequests {
		waitGroup.Add(1)
		go func(index int, request SQLMultiSourceSnapshotRequest) {
			defer waitGroup.Done()
			ingestor, err := NewSQLExternalSnapshotIngestor(SQLExternalSnapshotIngestorOptions{
				Source:      request.Source,
				Key:         request.Key,
				Kind:        request.Kind,
				MaxRows:     coordinator.maxRows,
				MaxOffsets:  coordinator.maxOffsets,
				MaxPageRows: coordinator.maxPageRows,
			})
			if err != nil {
				results[index].err = err
				cancel()
				return
			}
			snapshot, _, err := ingestor.captureSnapshot(captureContext, request.Provider, coordinator.maxRows, coordinator.maxOffsets, coordinator.maxPageRows, options.RequireSnapshotIDs)
			results[index] = sqlMultiSourceSnapshotCaptureResult{snapshot: snapshot, err: err}
			if err != nil {
				cancel()
			}
		}(index, request)
	}
	waitGroup.Wait()
	if err := firstSQLMultiSourceSnapshotError(results); err != nil {
		return SQLMultiSourceSnapshotResult{}, err
	}
	sources := make([]SQLExternalSnapshot, len(results))
	for index, result := range results {
		sources[index] = result.snapshot
	}
	sort.Slice(sources, func(left, right int) bool {
		return sqlMultiSourceSnapshotLess(sources[left], sources[right])
	})

	coordinator.mu.RLock()
	generation := coordinator.generation + 1
	coordinator.mu.RUnlock()
	if generation == 0 {
		generation = 1
	}
	snapshot := SQLMultiSourceSnapshot{Generation: generation, Sources: sources}
	if err := store.Commit(ctx, cloneSQLMultiSourceSnapshot(snapshot)); err != nil {
		return SQLMultiSourceSnapshotResult{}, fmt.Errorf("%w: %v", ErrSQLMultiSourceSnapshotCommit, err)
	}
	coordinator.mu.Lock()
	coordinator.snapshot = cloneSQLMultiSourceSnapshot(snapshot)
	coordinator.generation = generation
	view := newSQLMultiSourceSnapshotView(snapshot)
	coordinator.view = view
	coordinator.mu.Unlock()
	return SQLMultiSourceSnapshotResult{
		Snapshot:     cloneSQLMultiSourceSnapshot(snapshot),
		View:         view,
		Generation:   generation,
		Checkpointed: true,
	}, nil
}

// Snapshot returns a detached current snapshot and its generation.
func (coordinator *SQLMultiSourceSnapshotCoordinator) Snapshot() (SQLMultiSourceSnapshot, uint64) {
	if coordinator == nil {
		return SQLMultiSourceSnapshot{}, 0
	}
	coordinator.mu.RLock()
	snapshot, generation := cloneSQLMultiSourceSnapshot(coordinator.snapshot), coordinator.generation
	coordinator.mu.RUnlock()
	return snapshot, generation
}

// View returns the currently published immutable view.
func (coordinator *SQLMultiSourceSnapshotCoordinator) View() *SQLMultiSourceSnapshotView {
	if coordinator == nil {
		return nil
	}
	coordinator.mu.RLock()
	view := coordinator.view
	coordinator.mu.RUnlock()
	return view
}

// BeginSQLSnapshot pins the currently published immutable view for one SQL
// execution. A later capture publishes a new view pointer and cannot mutate
// the one returned here, so the release callback is intentionally a no-op.
func (coordinator *SQLMultiSourceSnapshotCoordinator) BeginSQLSnapshot(ctx context.Context) (SQLSourceResolver, func(), error) {
    if coordinator == nil {
        return nil, nil, ErrSQLMultiSourceSnapshotNil
    }
    if ctx == nil {
        ctx = context.Background()
    }
    if err := ctx.Err(); err != nil {
        return nil, nil, err
    }
    coordinator.mu.RLock()
    view := coordinator.view
    coordinator.mu.RUnlock()
    if view == nil {
        return nil, nil, ErrSQLMultiSourceSnapshotUnavailable
    }
    return view, func() {}, nil
}
// ResolveSQLSource resolves a source from the currently published view.
func (coordinator *SQLMultiSourceSnapshotCoordinator) ResolveSQLSource(kind, key string) ([]Row, error) {
	if coordinator == nil {
		return nil, ErrSQLMultiSourceSnapshotNil
	}
	coordinator.mu.RLock()
	view := coordinator.view
	coordinator.mu.RUnlock()
	if view == nil {
		return nil, ErrSQLMultiSourceSnapshotIdentity
	}
	return view.ResolveSQLSource(kind, key)
}

func (coordinator *SQLMultiSourceSnapshotCoordinator) normalizeRequests(requests []SQLMultiSourceSnapshotRequest) ([]SQLMultiSourceSnapshotRequest, error) {
	if len(requests) == 0 {
		return nil, ErrSQLMultiSourceSnapshotRequestInvalid
	}
	if len(requests) > coordinator.maxSources {
		return nil, ErrSQLMultiSourceSnapshotSourceLimit
	}
	normalized := make([]SQLMultiSourceSnapshotRequest, len(requests))
	seenSources := make(map[string]struct{}, len(requests))
	for index, request := range requests {
		ingestor, err := NewSQLExternalSnapshotIngestor(SQLExternalSnapshotIngestorOptions{
			Source: request.Source,
			Key:    request.Key,
			Kind:   request.Kind,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrSQLMultiSourceSnapshotRequestInvalid, err)
		}
		if _, exists := seenSources[ingestor.source]; exists {
			return nil, fmt.Errorf("%w: %s", ErrSQLMultiSourceSnapshotDuplicateSource, ingestor.source)
		}
		seenSources[ingestor.source] = struct{}{}
		normalized[index] = SQLMultiSourceSnapshotRequest{
			Source:   ingestor.source,
			Key:      ingestor.key,
			Kind:     ingestor.kind,
			Provider: request.Provider,
		}
	}
	sort.Slice(normalized, func(left, right int) bool {
		if normalized[left].Source != normalized[right].Source {
			return normalized[left].Source < normalized[right].Source
		}
		if normalized[left].Key != normalized[right].Key {
			return normalized[left].Key < normalized[right].Key
		}
		return normalized[left].Kind < normalized[right].Kind
	})
	return normalized, nil
}

func (coordinator *SQLMultiSourceSnapshotCoordinator) normalizeStoredSnapshot(snapshot SQLMultiSourceSnapshot, requireSnapshotID bool) (SQLMultiSourceSnapshot, error) {
	if snapshot.Generation == 0 || len(snapshot.Sources) == 0 || len(snapshot.Sources) > coordinator.maxSources {
		return SQLMultiSourceSnapshot{}, ErrSQLMultiSourceSnapshotInvalid
	}
	normalized := make([]SQLExternalSnapshot, len(snapshot.Sources))
	seenSources := make(map[string]struct{}, len(snapshot.Sources))
	seenIdentities := make(map[string]struct{}, len(snapshot.Sources))
	for index, source := range snapshot.Sources {
		ingestor, err := NewSQLExternalSnapshotIngestor(SQLExternalSnapshotIngestorOptions{
			Source:      source.Metadata.Source,
			Key:         source.Metadata.Key,
			Kind:        source.Metadata.Kind,
			MaxRows:     coordinator.maxRows,
			MaxOffsets:  coordinator.maxOffsets,
			MaxPageRows: coordinator.maxPageRows,
		})
		if err != nil {
			return SQLMultiSourceSnapshot{}, fmt.Errorf("%w: %v", ErrSQLMultiSourceSnapshotInvalid, err)
		}
		if _, exists := seenSources[ingestor.source]; exists {
			return SQLMultiSourceSnapshot{}, ErrSQLMultiSourceSnapshotDuplicateSource
		}
		identity := sqlMultiSourceSnapshotIdentityKey(ingestor.kind, ingestor.key)
		if _, exists := seenIdentities[identity]; exists {
			return SQLMultiSourceSnapshot{}, ErrSQLMultiSourceSnapshotIdentity
		}
		seenSources[ingestor.source] = struct{}{}
		seenIdentities[identity] = struct{}{}
		normalizedSource, err := ingestor.normalizeSnapshot(source, coordinator.maxRows, coordinator.maxOffsets, requireSnapshotID)
		if err != nil {
			return SQLMultiSourceSnapshot{}, err
		}
		normalized[index] = normalizedSource
	}
	sort.Slice(normalized, func(left, right int) bool {
		return sqlMultiSourceSnapshotLess(normalized[left], normalized[right])
	})
	return SQLMultiSourceSnapshot{Generation: snapshot.Generation, Sources: normalized}, nil
}

func newSQLMultiSourceSnapshotView(snapshot SQLMultiSourceSnapshot) *SQLMultiSourceSnapshotView {
	cloned := cloneSQLMultiSourceSnapshot(snapshot)
	indexes := make(map[sqlMultiSourceSnapshotLookupKey]int, len(cloned.Sources))
	for index, source := range cloned.Sources {
		kind := normalizeSQLMultiSourceSnapshotKind(source.Metadata.Kind)
		indexes[sqlMultiSourceSnapshotLookupKey{kind: kind, key: strings.TrimSpace(source.Metadata.Key)}] = index
	}
	return &SQLMultiSourceSnapshotView{snapshot: cloned, indexes: indexes}
}

func firstSQLMultiSourceSnapshotError(results []sqlMultiSourceSnapshotCaptureResult) error {
	var first error
	for _, result := range results {
		if result.err == nil {
			continue
		}
		if first == nil || (isSQLMultiSourceSnapshotContextError(first) && !isSQLMultiSourceSnapshotContextError(result.err)) {
			first = result.err
		}
	}
	return first
}

func isSQLMultiSourceSnapshotContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func sqlMultiSourceSnapshotLess(left, right SQLExternalSnapshot) bool {
	if left.Metadata.Source != right.Metadata.Source {
		return left.Metadata.Source < right.Metadata.Source
	}
	if left.Metadata.Key != right.Metadata.Key {
		return left.Metadata.Key < right.Metadata.Key
	}
	return left.Metadata.Kind < right.Metadata.Kind
}

func sqlMultiSourceSnapshotIdentityKey(kind, key string) string {
	return normalizeSQLMultiSourceSnapshotKind(kind) + "\x00" + strings.TrimSpace(key)
}

func normalizeSQLMultiSourceSnapshotKind(value string) string {
	value = strings.TrimSpace(value)
	for index := 0; index < len(value); index++ {
		if value[index] >= 'a' && value[index] <= 'z' {
			return strings.ToUpper(value)
		}
		if value[index] >= 0x80 {
			return strings.ToUpper(value)
		}
	}
	return value
}

func cloneSQLMultiSourceSnapshot(snapshot SQLMultiSourceSnapshot) SQLMultiSourceSnapshot {
	cloned := SQLMultiSourceSnapshot{Generation: snapshot.Generation}
	if len(snapshot.Sources) == 0 {
		return cloned
	}
	cloned.Sources = make([]SQLExternalSnapshot, len(snapshot.Sources))
	for index, source := range snapshot.Sources {
		cloned.Sources[index] = SQLExternalSnapshot{
			Metadata: SQLExternalSnapshotMetadata{
				Source:     source.Metadata.Source,
				Key:        source.Metadata.Key,
				Kind:       source.Metadata.Kind,
				SnapshotID: source.Metadata.SnapshotID,
				Offsets:    append([]SQLExternalSnapshotOffset(nil), source.Metadata.Offsets...),
			},
			Rows: cloneSQLExternalSnapshotRows(source.Rows),
		}
	}
	return cloned
}
