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
	DefaultSQLExternalSnapshotMaxRows     = 1_000_000
	DefaultSQLExternalSnapshotMaxOffsets  = 100_000
	DefaultSQLExternalSnapshotMaxPageRows = 10_000
	MaxSQLExternalSnapshotMaxRows         = 10_000_000
	MaxSQLExternalSnapshotMaxOffsets      = 1_000_000
	MaxSQLExternalSnapshotMaxPageRows     = 100_000
	MaxSQLExternalSnapshotLabelBytes      = 256
	MaxSQLExternalSnapshotIDBytes         = 512
)

var (
	ErrSQLExternalSnapshotNil                = errors.New("hatSql: external snapshot ingestor is nil")
	ErrSQLExternalSnapshotInvalid            = errors.New("hatSql: external snapshot is invalid")
	ErrSQLExternalSnapshotProviderRequired   = errors.New("hatSql: external snapshot provider is required")
	ErrSQLExternalSnapshotCheckpointRequired = errors.New("hatSql: external snapshot checkpoint store is required")
	ErrSQLExternalSnapshotAuthentication     = errors.New("hatSql: external snapshot authentication failed")
	ErrSQLExternalSnapshotProvider           = errors.New("hatSql: external snapshot provider failed")
	ErrSQLExternalSnapshotCheckpointLoad     = errors.New("hatSql: external snapshot checkpoint load failed")
	ErrSQLExternalSnapshotCheckpointCommit   = errors.New("hatSql: external snapshot checkpoint commit failed")
	ErrSQLExternalSnapshotIdentity           = errors.New("hatSql: external snapshot identity is incompatible")
	ErrSQLExternalSnapshotIDRequired         = errors.New("hatSql: external snapshot ID is required")
	ErrSQLExternalSnapshotRowsLimit          = errors.New("hatSql: external snapshot row limit exceeded")
	ErrSQLExternalSnapshotPageLimit          = errors.New("hatSql: external snapshot page limit exceeded")
	ErrSQLExternalSnapshotOffsetsLimit       = errors.New("hatSql: external snapshot offset limit exceeded")
	ErrSQLExternalSnapshotAlreadyInitialized = errors.New("hatSql: external snapshot ingestor is already initialized")
	ErrSQLExternalSnapshotOffsetInvalid      = errors.New("hatSql: external snapshot offset is invalid")
	ErrSQLExternalSnapshotOffsetDuplicate    = errors.New("hatSql: external snapshot offset is duplicated")
)

// SQLExternalSnapshotMetadata identifies a point-in-time source snapshot.
// Offsets describe the source positions at or before which the snapshot rows
// are complete, allowing a connector to start its change stream after the
// cutover without rereading the snapshot.
type SQLExternalSnapshotMetadata struct {
	Source     string                      `json:"source"`
	Key        string                      `json:"key"`
	Kind       string                      `json:"kind"`
	SnapshotID string                      `json:"snapshot_id"`
	Offsets    []SQLExternalSnapshotOffset `json:"offsets"`
}

// SQLExternalSnapshotOffset is a connector-neutral high-watermark. Partition
// is a string because PostgreSQL WAL positions, Kafka partitions, and CDC
// stream names do not share one native type.
type SQLExternalSnapshotOffset struct {
	Source    string `json:"source"`
	Partition string `json:"partition"`
	Offset    uint64 `json:"offset"`
}

// SQLExternalSnapshot is the durable payload exchanged between a provider,
// an ingestor, and a checkpoint store.
type SQLExternalSnapshot struct {
	Metadata SQLExternalSnapshotMetadata `json:"metadata"`
	Rows     []Row                       `json:"rows"`
}

// SQLExternalSnapshotProvider authenticates and streams one bounded snapshot
// into the supplied sink. Implementations may use Kafka, PostgreSQL, CDC, or
// another source without adding that client's dependency to hatSql.
type SQLExternalSnapshotProvider interface {
	Authenticate(context.Context) error
	Snapshot(context.Context, SQLExternalSnapshotSink) (SQLExternalSnapshotMetadata, error)
}

// SQLExternalSnapshotSink accepts bounded pages from a provider. AppendRows
// either accepts the complete page or leaves the collected snapshot unchanged.
type SQLExternalSnapshotSink interface {
	AppendRows([]Row) error
}

// SQLExternalSnapshotCheckpointStore atomically persists one complete source
// snapshot. Rows and metadata must become visible together or not at all.
type SQLExternalSnapshotCheckpointStore interface {
	Load(context.Context, string, string) (SQLExternalSnapshot, bool, error)
	Commit(context.Context, SQLExternalSnapshot) error
}

// SQLExternalSnapshotIngestorOptions identifies one logical SQL source and
// configures its default memory bounds.
type SQLExternalSnapshotIngestorOptions struct {
	Source      string
	Key         string
	Kind        string
	MaxRows     int
	MaxOffsets  int
	MaxPageRows int
}

// SQLExternalSnapshotIngestOptions controls one bootstrap attempt. Zero
// limits use the ingestor defaults. Authentication and checkpoint recovery are
// always enabled; there is no insecure unauthenticated mode.
type SQLExternalSnapshotIngestOptions struct {
	MaxRows              int
	MaxOffsets           int
	MaxPageRows          int
	RequireSnapshotID    bool
	AllowReplaceExisting bool
}

// SQLExternalSnapshotIngestResult reports the committed or restored cutover.
type SQLExternalSnapshotIngestResult struct {
	SnapshotID   string
	Rows         int
	Offsets      int
	Pages        int
	Checkpointed bool
	Restored     bool
}

// SQLExternalSnapshotIngestor exposes an atomically replaced external source
// snapshot through SourceResolver. Normal SQL reads only take a read lock and
// never contact the external provider.
type SQLExternalSnapshotIngestor struct {
	mu          sync.RWMutex
	source      string
	key         string
	kind        string
	maxRows     int
	maxOffsets  int
	maxPageRows int
	rows        []Row
	offsets     []SQLExternalSnapshotOffset
	snapshotID  string
	generation  uint64
}

var _ SourceResolver = (*SQLExternalSnapshotIngestor)(nil)

// NewSQLExternalSnapshotIngestor creates an empty, bounded external source.
func NewSQLExternalSnapshotIngestor(options SQLExternalSnapshotIngestorOptions) (*SQLExternalSnapshotIngestor, error) {
	source, err := normalizeSQLExternalSnapshotLabel(options.Source, "source")
	if err != nil {
		return nil, err
	}
	key, err := normalizeSQLExternalSnapshotLabel(options.Key, "key")
	if err != nil {
		return nil, err
	}
	kind := strings.ToUpper(strings.TrimSpace(options.Kind))
	if kind == "" {
		kind = "EXTERNAL"
	}
	kind, err = normalizeSQLExternalSnapshotLabel(kind, "kind")
	if err != nil {
		return nil, err
	}
	maxRows, maxOffsets, maxPageRows := options.MaxRows, options.MaxOffsets, options.MaxPageRows
	if maxRows == 0 {
		maxRows = DefaultSQLExternalSnapshotMaxRows
	}
	if maxOffsets == 0 {
		maxOffsets = DefaultSQLExternalSnapshotMaxOffsets
	}
	if maxPageRows == 0 {
		maxPageRows = DefaultSQLExternalSnapshotMaxPageRows
	}
	maxRows, maxOffsets, maxPageRows, err = normalizeSQLExternalSnapshotLimits(maxRows, maxOffsets, maxPageRows)
	if err != nil {
		return nil, err
	}
	return &SQLExternalSnapshotIngestor{
		source:      source,
		key:         key,
		kind:        kind,
		maxRows:     maxRows,
		maxOffsets:  maxOffsets,
		maxPageRows: maxPageRows,
	}, nil
}

// ResolveSQLSource returns an independent row snapshot for the configured
// source identity.
func (ingestor *SQLExternalSnapshotIngestor) ResolveSQLSource(name, key string) ([]Row, error) {
	if ingestor == nil {
		return nil, ErrSQLExternalSnapshotNil
	}
	if !strings.EqualFold(strings.TrimSpace(name), ingestor.kind) || strings.TrimSpace(key) != ingestor.key {
		return nil, ErrSQLExternalSnapshotIdentity
	}
	ingestor.mu.RLock()
	rows := cloneSQLExternalSnapshotRows(ingestor.rows)
	ingestor.mu.RUnlock()
	return rows, nil
}

// Snapshot returns an independent checkpoint payload and its publication
// generation.
func (ingestor *SQLExternalSnapshotIngestor) Snapshot() (SQLExternalSnapshot, uint64) {
	if ingestor == nil {
		return SQLExternalSnapshot{}, 0
	}
	ingestor.mu.RLock()
	snapshot, generation := ingestor.snapshotLocked(), ingestor.generation
	ingestor.mu.RUnlock()
	return snapshot, generation
}

// Offsets returns deterministic copies of the current source high-watermarks.
func (ingestor *SQLExternalSnapshotIngestor) Offsets() []SQLExternalSnapshotOffset {
	if ingestor == nil {
		return nil
	}
	ingestor.mu.RLock()
	offsets := append([]SQLExternalSnapshotOffset(nil), ingestor.offsets...)
	ingestor.mu.RUnlock()
	return offsets
}

// Generation returns the number of successfully published snapshots.
func (ingestor *SQLExternalSnapshotIngestor) Generation() uint64 {
	if ingestor == nil {
		return 0
	}
	ingestor.mu.RLock()
	generation := ingestor.generation
	ingestor.mu.RUnlock()
	return generation
}

// IngestSnapshotWithCheckpoint authenticates a provider, collects a bounded
// page stream, validates its identity and offsets, then atomically publishes
// it with a durable checkpoint. If a checkpoint already exists, it is restored
// without contacting the external provider.
func (ingestor *SQLExternalSnapshotIngestor) IngestSnapshotWithCheckpoint(ctx context.Context, provider SQLExternalSnapshotProvider, store SQLExternalSnapshotCheckpointStore, options SQLExternalSnapshotIngestOptions) (SQLExternalSnapshotIngestResult, error) {
	if ingestor == nil {
		return SQLExternalSnapshotIngestResult{}, ErrSQLExternalSnapshotNil
	}
	if store == nil {
		return SQLExternalSnapshotIngestResult{}, ErrSQLExternalSnapshotCheckpointRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SQLExternalSnapshotIngestResult{}, err
	}
	maxRows, maxOffsets, maxPageRows, err := ingestor.limits(options)
	if err != nil {
		return SQLExternalSnapshotIngestResult{}, err
	}
	stored, found, err := store.Load(ctx, ingestor.source, ingestor.key)
	if err != nil {
		return SQLExternalSnapshotIngestResult{}, ErrSQLExternalSnapshotCheckpointLoad
	}
	if found {
		normalized, err := ingestor.normalizeSnapshot(stored, maxRows, maxOffsets, options.RequireSnapshotID)
		if err != nil {
			return SQLExternalSnapshotIngestResult{}, err
		}
		ingestor.mu.Lock()
		ingestor.installSnapshotLocked(normalized)
		ingestor.mu.Unlock()
		return SQLExternalSnapshotIngestResult{
			SnapshotID: normalized.Metadata.SnapshotID,
			Rows:       len(normalized.Rows),
			Offsets:    len(normalized.Metadata.Offsets),
			Restored:   true,
		}, nil
	}
	if provider == nil {
		return SQLExternalSnapshotIngestResult{}, ErrSQLExternalSnapshotProviderRequired
	}
	if err := provider.Authenticate(ctx); err != nil {
		return SQLExternalSnapshotIngestResult{}, ErrSQLExternalSnapshotAuthentication
	}
	if err := ctx.Err(); err != nil {
		return SQLExternalSnapshotIngestResult{}, err
	}
	collector := &sqlExternalSnapshotCollector{maxRows: maxRows, maxPageRows: maxPageRows}
	metadata, err := provider.Snapshot(ctx, collector)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return SQLExternalSnapshotIngestResult{}, err
		}
		if errors.Is(err, ErrSQLExternalSnapshotRowsLimit) || errors.Is(err, ErrSQLExternalSnapshotPageLimit) || errors.Is(err, ErrSQLExternalSnapshotInvalid) {
			return SQLExternalSnapshotIngestResult{}, err
		}
		return SQLExternalSnapshotIngestResult{}, ErrSQLExternalSnapshotProvider
	}
	if err := ctx.Err(); err != nil {
		return SQLExternalSnapshotIngestResult{}, err
	}
	snapshot, err := ingestor.normalizeSnapshot(SQLExternalSnapshot{Metadata: metadata, Rows: collector.rows}, maxRows, maxOffsets, options.RequireSnapshotID)
	if err != nil {
		return SQLExternalSnapshotIngestResult{}, err
	}
	ingestor.mu.Lock()
	if ingestor.hasStateLocked() && !options.AllowReplaceExisting {
		ingestor.mu.Unlock()
		return SQLExternalSnapshotIngestResult{}, ErrSQLExternalSnapshotAlreadyInitialized
	}
	previous, previousGeneration := ingestor.snapshotLocked(), ingestor.generation
	ingestor.installSnapshotLocked(snapshot)
	committedSnapshot := ingestor.snapshotLocked()
	if err := store.Commit(ctx, committedSnapshot); err != nil {
		ingestor.restoreSnapshotLocked(previous, previousGeneration)
		ingestor.mu.Unlock()
		return SQLExternalSnapshotIngestResult{}, ErrSQLExternalSnapshotCheckpointCommit
	}
	ingestor.mu.Unlock()
	return SQLExternalSnapshotIngestResult{
		SnapshotID:   snapshot.Metadata.SnapshotID,
		Rows:         len(snapshot.Rows),
		Offsets:      len(snapshot.Metadata.Offsets),
		Pages:        collector.pages,
		Checkpointed: true,
	}, nil
}

type sqlExternalSnapshotCollector struct {
	rows        []Row
	maxRows     int
	maxPageRows int
	pages       int
}

func (collector *sqlExternalSnapshotCollector) AppendRows(rows []Row) error {
	if collector == nil {
		return ErrSQLExternalSnapshotNil
	}
	if len(rows) > collector.maxPageRows {
		return ErrSQLExternalSnapshotPageLimit
	}
	if len(rows) > collector.maxRows-len(collector.rows) {
		return ErrSQLExternalSnapshotRowsLimit
	}
	for _, row := range rows {
		if row == nil {
			return ErrSQLExternalSnapshotInvalid
		}
	}
	for _, row := range rows {
		collector.rows = append(collector.rows, cloneSQLExternalSnapshotRow(row))
	}
	if len(rows) > 0 {
		collector.pages++
	}
	return nil
}

func (ingestor *SQLExternalSnapshotIngestor) limits(options SQLExternalSnapshotIngestOptions) (int, int, int, error) {
	maxRows, maxOffsets, maxPageRows := options.MaxRows, options.MaxOffsets, options.MaxPageRows
	if maxRows == 0 {
		maxRows = ingestor.maxRows
	}
	if maxOffsets == 0 {
		maxOffsets = ingestor.maxOffsets
	}
	if maxPageRows == 0 {
		maxPageRows = ingestor.maxPageRows
	}
	return normalizeSQLExternalSnapshotLimits(maxRows, maxOffsets, maxPageRows)
}

func (ingestor *SQLExternalSnapshotIngestor) normalizeSnapshot(snapshot SQLExternalSnapshot, maxRows, maxOffsets int, requireID bool) (SQLExternalSnapshot, error) {
	metadata := snapshot.Metadata
	metadata.Source = strings.TrimSpace(metadata.Source)
	metadata.Key = strings.TrimSpace(metadata.Key)
	metadata.Kind = strings.ToUpper(strings.TrimSpace(metadata.Kind))
	metadata.SnapshotID = strings.TrimSpace(metadata.SnapshotID)
	if metadata.Source != ingestor.source || metadata.Key != ingestor.key || metadata.Kind != ingestor.kind {
		return SQLExternalSnapshot{}, ErrSQLExternalSnapshotIdentity
	}
	if metadata.SnapshotID == "" && requireID {
		return SQLExternalSnapshot{}, ErrSQLExternalSnapshotIDRequired
	}
	if len(metadata.SnapshotID) > MaxSQLExternalSnapshotIDBytes || strings.IndexByte(metadata.SnapshotID, 0) >= 0 {
		return SQLExternalSnapshot{}, ErrSQLExternalSnapshotInvalid
	}
	if len(snapshot.Rows) > maxRows {
		return SQLExternalSnapshot{}, ErrSQLExternalSnapshotRowsLimit
	}
	for _, row := range snapshot.Rows {
		if row == nil {
			return SQLExternalSnapshot{}, ErrSQLExternalSnapshotInvalid
		}
	}
	offsets, err := normalizeSQLExternalSnapshotOffsets(metadata.Offsets, ingestor.source, maxOffsets)
	if err != nil {
		return SQLExternalSnapshot{}, err
	}
	metadata.Offsets = offsets
	return SQLExternalSnapshot{Metadata: metadata, Rows: cloneSQLExternalSnapshotRows(snapshot.Rows)}, nil
}

func (ingestor *SQLExternalSnapshotIngestor) hasStateLocked() bool {
	return len(ingestor.rows) > 0 || len(ingestor.offsets) > 0 || ingestor.snapshotID != ""
}

func (ingestor *SQLExternalSnapshotIngestor) snapshotLocked() SQLExternalSnapshot {
	return SQLExternalSnapshot{
		Metadata: SQLExternalSnapshotMetadata{
			Source:     ingestor.source,
			Key:        ingestor.key,
			Kind:       ingestor.kind,
			SnapshotID: ingestor.snapshotID,
			Offsets:    append([]SQLExternalSnapshotOffset(nil), ingestor.offsets...),
		},
		Rows: cloneSQLExternalSnapshotRows(ingestor.rows),
	}
}

func (ingestor *SQLExternalSnapshotIngestor) installSnapshotLocked(snapshot SQLExternalSnapshot) {
	ingestor.rows = cloneSQLExternalSnapshotRows(snapshot.Rows)
	ingestor.offsets = append([]SQLExternalSnapshotOffset(nil), snapshot.Metadata.Offsets...)
	ingestor.snapshotID = snapshot.Metadata.SnapshotID
	ingestor.generation++
}

func (ingestor *SQLExternalSnapshotIngestor) restoreSnapshotLocked(snapshot SQLExternalSnapshot, generation uint64) {
	ingestor.rows = cloneSQLExternalSnapshotRows(snapshot.Rows)
	ingestor.offsets = append([]SQLExternalSnapshotOffset(nil), snapshot.Metadata.Offsets...)
	ingestor.snapshotID = snapshot.Metadata.SnapshotID
	ingestor.generation = generation
}

func normalizeSQLExternalSnapshotLimits(maxRows, maxOffsets, maxPageRows int) (int, int, int, error) {
	if maxRows < 1 || maxRows > MaxSQLExternalSnapshotMaxRows || maxOffsets < 1 || maxOffsets > MaxSQLExternalSnapshotMaxOffsets || maxPageRows < 1 || maxPageRows > MaxSQLExternalSnapshotMaxPageRows {
		return 0, 0, 0, ErrSQLExternalSnapshotInvalid
	}
	return maxRows, maxOffsets, maxPageRows, nil
}

func normalizeSQLExternalSnapshotLabel(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > MaxSQLExternalSnapshotLabelBytes || strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("%w: %s", ErrSQLExternalSnapshotInvalid, label)
	}
	return value, nil
}

func normalizeSQLExternalSnapshotOffsets(offsets []SQLExternalSnapshotOffset, source string, maxOffsets int) ([]SQLExternalSnapshotOffset, error) {
	if len(offsets) > maxOffsets {
		return nil, ErrSQLExternalSnapshotOffsetsLimit
	}
	normalized := make([]SQLExternalSnapshotOffset, len(offsets))
	seen := make(map[string]struct{}, len(offsets))
	for index, offset := range offsets {
		offset.Source = strings.TrimSpace(offset.Source)
		offset.Partition = strings.TrimSpace(offset.Partition)
		if offset.Source != source || offset.Partition == "" || len(offset.Partition) > MaxSQLExternalSnapshotLabelBytes || strings.IndexByte(offset.Partition, 0) >= 0 {
			return nil, ErrSQLExternalSnapshotOffsetInvalid
		}
		key := offset.Source + "\x00" + offset.Partition
		if _, exists := seen[key]; exists {
			return nil, ErrSQLExternalSnapshotOffsetDuplicate
		}
		seen[key] = struct{}{}
		normalized[index] = offset
	}
	sort.Slice(normalized, func(left, right int) bool {
		if normalized[left].Source != normalized[right].Source {
			return normalized[left].Source < normalized[right].Source
		}
		return normalized[left].Partition < normalized[right].Partition
	})
	return normalized, nil
}

func cloneSQLExternalSnapshotRows(rows []Row) []Row {
	if len(rows) == 0 {
		return nil
	}
	cloned := make([]Row, len(rows))
	for index, row := range rows {
		cloned[index] = cloneSQLExternalSnapshotRow(row)
	}
	return cloned
}

func cloneSQLExternalSnapshotRow(row Row) Row {
	if row == nil {
		return nil
	}
	cloned := make(Row, len(row))
	for key, value := range row {
		cloned[key] = cloneSQLExternalSnapshotValue(value)
	}
	return cloned
}

func cloneSQLExternalSnapshotValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case Row:
		return cloneSQLExternalSnapshotRow(typed)
	case map[string]interface{}:
		cloned := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			cloned[key] = cloneSQLExternalSnapshotValue(nested)
		}
		return cloned
	case []interface{}:
		cloned := make([]interface{}, len(typed))
		for index, nested := range typed {
			cloned[index] = cloneSQLExternalSnapshotValue(nested)
		}
		return cloned
	case []byte:
		return append([]byte(nil), typed...)
	default:
		return value
	}
}
