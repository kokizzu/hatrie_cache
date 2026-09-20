package hatSql

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ErrJSONSubcolumnAutoMaterializerConfig identifies invalid automatic
// materializer limits.
var ErrJSONSubcolumnAutoMaterializerConfig = errors.New("hatriecache: invalid automatic JSON subcolumn materializer configuration")

const (
	// DefaultJSONSubcolumnAutoMinObservations avoids paying materialization cost
	// for paths that are only read once or twice.
	DefaultJSONSubcolumnAutoMinObservations = 3
	DefaultJSONSubcolumnAutoMaxEntries      = 64
	DefaultJSONSubcolumnAutoMaxRows         = 1 << 20
	DefaultJSONSubcolumnAutoMaxBytes        = 64 << 20
)

// JSONSubcolumnAutoMaterializerOptions bounds the opt-in automatic typed JSON
// subcolumn cache. A zero value selects the documented sane default for every
// limit.
type JSONSubcolumnAutoMaterializerOptions struct {
	MinObservations int
	MaxEntries      int
	MaxRows         int
	MaxBytes        uint64
}

// JSONSubcolumnAutoSource identifies one source snapshot. Generation must
// change whenever the source contents change; it prevents a materialized
// column from being reused for a newer snapshot.
type JSONSubcolumnAutoSource struct {
	SourceName string
	SourceKey  string
	Generation uint64
}

// JSONSubcolumnAutoKey identifies one typed path in one source snapshot.
// Path is normalized before it is stored or looked up.
type JSONSubcolumnAutoKey struct {
	SourceName string
	SourceKey  string
	Field      string
	Path       string
	Generation uint64
}

// JSONSubcolumnAutoMaterializerStats reports bounded lifecycle counters. It
// contains no source values, paths, or query text.
type JSONSubcolumnAutoMaterializerStats struct {
	Entries       int
	Observations  uint64
	Promotions    uint64
	Rejections    uint64
	Evictions     uint64
	Hits          uint64
	RetainedBytes uint64
}

// JSONSubcolumnAutoMaterializer promotes repeatedly observed JSON paths to
// compact immutable columns. It retains only promoted columns and bounded
// metadata; source documents are used during Observe and then discarded.
type JSONSubcolumnAutoMaterializer struct {
	mu              sync.RWMutex
	minObservations int
	maxEntries      int
	maxRows         int
	maxBytes        uint64
	entries         map[jsonSubcolumnAutoLogicalKey]*jsonSubcolumnAutoEntry
	sequence        uint64
	observations    uint64
	promotions      uint64
	rejections      uint64
	evictions       uint64
	retainedBytes   uint64
	hits            atomic.Uint64
}

type jsonSubcolumnAutoLogicalKey struct {
	sourceName string
	sourceKey  string
	field      string
	path       string
}

type jsonSubcolumnAutoEntry struct {
	generation    uint64
	observations  int
	sequence      uint64
	rejected      bool
	column        ColumnarJSONSubcolumn
	retainedBytes uint64
}

type jsonSubcolumnAutoResolvedColumn struct {
	key    ColumnarJSONSubcolumnKey
	column ColumnarJSONSubcolumn
}

// NewJSONSubcolumnAutoMaterializer creates an opt-in bounded materializer.
// The default requires three observations, retains at most 64 paths, accepts
// at most 1 MiB rows per path, and retains at most 64 MiB of typed columns.
func NewJSONSubcolumnAutoMaterializer(options JSONSubcolumnAutoMaterializerOptions) (*JSONSubcolumnAutoMaterializer, error) {
	if options.MinObservations < 0 || options.MaxEntries < 0 || options.MaxRows < 0 {
		return nil, ErrJSONSubcolumnAutoMaterializerConfig
	}
	if options.MinObservations == 0 {
		options.MinObservations = DefaultJSONSubcolumnAutoMinObservations
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = DefaultJSONSubcolumnAutoMaxEntries
	}
	if options.MaxRows == 0 {
		options.MaxRows = DefaultJSONSubcolumnAutoMaxRows
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = DefaultJSONSubcolumnAutoMaxBytes
	}
	if options.MinObservations < 1 || options.MaxEntries < 1 || options.MaxRows < 1 || options.MaxBytes < 1 {
		return nil, ErrJSONSubcolumnAutoMaterializerConfig
	}
	return &JSONSubcolumnAutoMaterializer{
		minObservations: options.MinObservations,
		maxEntries:      options.MaxEntries,
		maxRows:         options.MaxRows,
		maxBytes:        options.MaxBytes,
		entries:         make(map[jsonSubcolumnAutoLogicalKey]*jsonSubcolumnAutoEntry),
	}, nil
}

// Observe records one source observation and returns a promoted column once
// the configured threshold is reached. A false result means the caller should
// use the ordinary JSON path. Invalid or mixed-type documents are rejected for
// this source generation and also return false so query semantics fall back.
func (materializer *JSONSubcolumnAutoMaterializer) Observe(key JSONSubcolumnAutoKey, documents []interface{}) (ColumnarJSONSubcolumn, bool, error) {
	if materializer == nil {
		return ColumnarJSONSubcolumn{}, false, nil
	}
	key, logical, err := normalizeJSONSubcolumnAutoKey(key)
	if err != nil {
		return ColumnarJSONSubcolumn{}, false, err
	}
	if len(documents) == 0 || len(documents) > materializer.maxRows {
		return ColumnarJSONSubcolumn{}, false, nil
	}
	materializer.mu.Lock()
	column, ready := materializer.observeCanonicalLocked(key, logical, documents)
	materializer.mu.Unlock()
	return column, ready, nil
}

// ObserveRequests records one access observation for each distinct requested
// path and reports whether row data should now be loaded for promotion. It
// deliberately does not inspect documents, allowing a source adapter to keep
// cold fallback queries on their original decode path.
func (materializer *JSONSubcolumnAutoMaterializer) ObserveRequests(source JSONSubcolumnAutoSource, paths []ColumnarJSONSubcolumnRequest) (bool, error) {
	if materializer == nil || len(paths) == 0 {
		return false, nil
	}
	type requestKey struct {
		key     JSONSubcolumnAutoKey
		logical jsonSubcolumnAutoLogicalKey
	}
	requests := make([]requestKey, 0, len(paths))
	seen := make(map[ColumnarJSONSubcolumnKey]struct{}, len(paths))
	for _, request := range paths {
		key := JSONSubcolumnAutoKey{
			SourceName: source.SourceName,
			SourceKey:  source.SourceKey,
			Field:      request.Field,
			Path:       request.Path,
			Generation: source.Generation,
		}
		key, logical, err := normalizeJSONSubcolumnAutoKey(key)
		if err != nil {
			return false, err
		}
		columnKey := ColumnarJSONSubcolumnKey{Field: key.Field, Path: key.Path}
		if _, duplicate := seen[columnKey]; duplicate {
			continue
		}
		seen[columnKey] = struct{}{}
		requests = append(requests, requestKey{key: key, logical: logical})
	}
	if len(requests) == 0 {
		return false, nil
	}
	ready := true
	materializer.mu.Lock()
	for _, request := range requests {
		entry := materializer.entries[request.logical]
		if entry != nil && entry.generation != request.key.Generation {
			materializer.removeEntryLocked(request.logical, entry)
			entry = nil
		}
		if entry == nil {
			entry = materializer.newEntryLocked(request.logical, request.key.Generation)
		}
		if entry.column.Rows != 0 {
			continue
		}
		if entry.rejected {
			ready = false
			continue
		}
		if entry.observations < materializer.minObservations {
			entry.observations++
			materializer.observations++
		}
		if entry.observations < materializer.minObservations {
			ready = false
		}
	}
	materializer.mu.Unlock()
	return ready, nil
}

func (materializer *JSONSubcolumnAutoMaterializer) observeCanonicalLocked(key JSONSubcolumnAutoKey, logical jsonSubcolumnAutoLogicalKey, documents []interface{}) (ColumnarJSONSubcolumn, bool) {
	entry := materializer.entries[logical]
	if entry != nil && entry.generation != key.Generation {
		materializer.removeEntryLocked(logical, entry)
		entry = nil
	}
	if entry == nil {
		entry = materializer.newEntryLocked(logical, key.Generation)
	}
	if entry.column.Rows != 0 {
		materializer.hits.Add(1)
		return entry.column, true
	}
	if entry.rejected {
		return ColumnarJSONSubcolumn{}, false
	}
	if entry.observations < materializer.minObservations {
		entry.observations++
		materializer.observations++
	}
	if entry.observations < materializer.minObservations {
		return ColumnarJSONSubcolumn{}, false
	}
	column, err := MaterializeJSONSubcolumn(key.Path, documents)
	if err != nil {
		entry.rejected = true
		materializer.rejections++
		return ColumnarJSONSubcolumn{}, false
	}
	retainedBytes := columnarJSONSubcolumnRetainedBytes(column)
	if retainedBytes > materializer.maxBytes {
		entry.rejected = true
		materializer.rejections++
		return ColumnarJSONSubcolumn{}, false
	}
	entry.column = column
	entry.retainedBytes = retainedBytes
	materializer.retainedBytes += retainedBytes
	materializer.promotions++
	return column, true
}

// Lookup returns a promoted immutable column for the exact source generation.
// It does not update recency metadata, keeping the common read path small and
// avoiding a write lock.
func (materializer *JSONSubcolumnAutoMaterializer) Lookup(key JSONSubcolumnAutoKey) (ColumnarJSONSubcolumn, bool, error) {
	if materializer == nil {
		return ColumnarJSONSubcolumn{}, false, nil
	}
	key, logical, err := normalizeJSONSubcolumnAutoKey(key)
	if err != nil {
		return ColumnarJSONSubcolumn{}, false, err
	}
	return materializer.lookupCanonical(key, logical)
}

func (materializer *JSONSubcolumnAutoMaterializer) lookupCanonical(key JSONSubcolumnAutoKey, logical jsonSubcolumnAutoLogicalKey) (ColumnarJSONSubcolumn, bool, error) {
	materializer.mu.RLock()
	entry := materializer.entries[logical]
	if entry == nil || entry.generation != key.Generation || entry.column.Rows == 0 {
		materializer.mu.RUnlock()
		return ColumnarJSONSubcolumn{}, false, nil
	}
	column := entry.column
	materializer.mu.RUnlock()
	materializer.hits.Add(1)
	return column, true, nil
}

// ResolveBatch builds the ColumnarBatch expected by
// ColumnarJSONSubcolumnSourceResolver. Cold paths return available=false so
// the caller keeps ordinary row execution; once all requested paths are
// promoted, the returned batch contains copied ordinary fields and compact
// typed JSON subcolumns. Duplicate requests are counted only once.
func (materializer *JSONSubcolumnAutoMaterializer) ResolveBatch(source JSONSubcolumnAutoSource, fields []string, paths []ColumnarJSONSubcolumnRequest, rows []Row) (ColumnarBatch, bool, error) {
	if materializer == nil || len(paths) == 0 || len(rows) == 0 || len(rows) > materializer.maxRows {
		return ColumnarBatch{}, false, nil
	}
	resolved := make([]jsonSubcolumnAutoResolvedColumn, 0, len(paths))
	seen := make(map[ColumnarJSONSubcolumnKey]struct{}, len(paths))
	for _, request := range paths {
		key := JSONSubcolumnAutoKey{
			SourceName: source.SourceName,
			SourceKey:  source.SourceKey,
			Field:      request.Field,
			Path:       request.Path,
			Generation: source.Generation,
		}
		key, logical, err := normalizeJSONSubcolumnAutoKey(key)
		if err != nil {
			return ColumnarBatch{}, false, err
		}
		columnKey := ColumnarJSONSubcolumnKey{Field: key.Field, Path: key.Path}
		if _, duplicate := seen[columnKey]; duplicate {
			continue
		}
		seen[columnKey] = struct{}{}
		column, found, err := materializer.lookupCanonical(key, logical)
		if err != nil {
			return ColumnarBatch{}, false, err
		}
		if !found {
			documents := make([]interface{}, len(rows))
			for rowIndex, row := range rows {
				documents[rowIndex] = row[key.Field]
			}
			materializer.mu.Lock()
			column, found = materializer.observeCanonicalLocked(key, logical, documents)
			materializer.mu.Unlock()
		}
		if !found {
			return ColumnarBatch{}, false, nil
		}
		resolved = append(resolved, jsonSubcolumnAutoResolvedColumn{key: columnKey, column: column})
	}
	if len(resolved) == 0 {
		return ColumnarBatch{}, false, nil
	}
	batch := ColumnarBatch{
		Rows:           len(rows),
		JSONSubcolumns: make(map[ColumnarJSONSubcolumnKey]ColumnarJSONSubcolumn, len(resolved)),
	}
	for _, resolvedColumn := range resolved {
		batch.JSONSubcolumns[resolvedColumn.key] = resolvedColumn.column
	}
	if len(fields) != 0 {
		batch.Columns = make(map[string][]interface{}, len(fields))
		for _, field := range fields {
			if _, exists := batch.Columns[field]; exists {
				continue
			}
			values := make([]interface{}, len(rows))
			for rowIndex, row := range rows {
				values[rowIndex] = row[field]
			}
			batch.Columns[field] = values
		}
	}
	return batch, true, nil
}

// ResolvePromotedBatch returns a batch for paths that are already promoted.
// It does not need source rows, so callers serving path-only queries can avoid
// retaining or decoding the complete JSON document set on every warm read.
func (materializer *JSONSubcolumnAutoMaterializer) ResolvePromotedBatch(source JSONSubcolumnAutoSource, paths []ColumnarJSONSubcolumnRequest) (ColumnarBatch, bool, error) {
	if materializer == nil || len(paths) == 0 {
		return ColumnarBatch{}, false, nil
	}
	resolved := make([]jsonSubcolumnAutoResolvedColumn, 0, len(paths))
	seen := make(map[ColumnarJSONSubcolumnKey]struct{}, len(paths))
	rows := -1
	for _, request := range paths {
		key := JSONSubcolumnAutoKey{
			SourceName: source.SourceName,
			SourceKey:  source.SourceKey,
			Field:      request.Field,
			Path:       request.Path,
			Generation: source.Generation,
		}
		key, _, err := normalizeJSONSubcolumnAutoKey(key)
		if err != nil {
			return ColumnarBatch{}, false, err
		}
		columnKey := ColumnarJSONSubcolumnKey{Field: key.Field, Path: key.Path}
		if _, duplicate := seen[columnKey]; duplicate {
			continue
		}
		seen[columnKey] = struct{}{}
		column, found, err := materializer.Lookup(key)
		if err != nil {
			return ColumnarBatch{}, false, err
		}
		if !found {
			return ColumnarBatch{}, false, nil
		}
		if rows < 0 {
			rows = column.Rows
		} else if rows != column.Rows {
			return ColumnarBatch{}, false, fmt.Errorf("%w: promoted JSON subcolumns have different row counts", ErrColumnarJSONSubcolumnInvalid)
		}
		resolved = append(resolved, jsonSubcolumnAutoResolvedColumn{key: columnKey, column: column})
	}
	if rows < 0 || len(resolved) == 0 {
		return ColumnarBatch{}, false, nil
	}
	batch := ColumnarBatch{
		Rows:           rows,
		JSONSubcolumns: make(map[ColumnarJSONSubcolumnKey]ColumnarJSONSubcolumn, len(resolved)),
	}
	for _, resolvedColumn := range resolved {
		batch.JSONSubcolumns[resolvedColumn.key] = resolvedColumn.column
	}
	return batch, true, nil
}

// InvalidateSource drops every promoted path for one source, regardless of
// generation. It is useful when a source cannot expose a monotonically
// increasing generation but can signal replacement explicitly.
func (materializer *JSONSubcolumnAutoMaterializer) InvalidateSource(sourceName, sourceKey string) {
	if materializer == nil {
		return
	}
	materializer.mu.Lock()
	for logical, entry := range materializer.entries {
		if logical.sourceName == sourceName && logical.sourceKey == sourceKey {
			materializer.removeEntryLocked(logical, entry)
		}
	}
	materializer.mu.Unlock()
}

// Stats returns bounded counters and the currently retained typed-column
// payload estimate.
func (materializer *JSONSubcolumnAutoMaterializer) Stats() JSONSubcolumnAutoMaterializerStats {
	if materializer == nil {
		return JSONSubcolumnAutoMaterializerStats{}
	}
	materializer.mu.RLock()
	stats := JSONSubcolumnAutoMaterializerStats{
		Entries:       len(materializer.entries),
		Observations:  materializer.observations,
		Promotions:    materializer.promotions,
		Rejections:    materializer.rejections,
		Evictions:     materializer.evictions,
		RetainedBytes: materializer.retainedBytes,
		Hits:          materializer.hits.Load(),
	}
	materializer.mu.RUnlock()
	return stats
}

func normalizeJSONSubcolumnAutoKey(key JSONSubcolumnAutoKey) (JSONSubcolumnAutoKey, jsonSubcolumnAutoLogicalKey, error) {
	if strings.TrimSpace(key.Field) == "" {
		return JSONSubcolumnAutoKey{}, jsonSubcolumnAutoLogicalKey{}, fmt.Errorf("%w: field is empty", ErrColumnarJSONSubcolumnInvalid)
	}
	path := strings.TrimSpace(key.Path)
	if !isSimpleCanonicalJSONSubcolumnPath(path) {
		var err error
		path, err = NormalizeJSONPath(path)
		if err != nil {
			return JSONSubcolumnAutoKey{}, jsonSubcolumnAutoLogicalKey{}, err
		}
	}
	key.Path = path
	logical := jsonSubcolumnAutoLogicalKey{
		sourceName: key.SourceName,
		sourceKey:  key.SourceKey,
		field:      key.Field,
		path:       key.Path,
	}
	return key, logical, nil
}

// isSimpleCanonicalJSONSubcolumnPath covers the common dot-member and numeric
// array paths without invoking the allocating general path parser. Complex
// member names still use NormalizeJSONPath and retain its full validation.
func isSimpleCanonicalJSONSubcolumnPath(path string) bool {
	if len(path) == 0 || path[0] != '$' || path != strings.TrimSpace(path) {
		return false
	}
	for index := 1; index < len(path); {
		switch path[index] {
		case '.':
			index++
			start := index
			for index < len(path) && isSimpleJSONSubcolumnMemberByte(path[index]) {
				index++
			}
			if index == start || !isSimpleJSONSubcolumnMemberStart(path[start]) {
				return false
			}
		case '[':
			index++
			start := index
			for index < len(path) && path[index] >= '0' && path[index] <= '9' {
				index++
			}
			if index == start || index-start > 1 && path[start] == '0' || index >= len(path) || path[index] != ']' {
				return false
			}
			index++
		default:
			return false
		}
	}
	return true
}

func isSimpleJSONSubcolumnMemberByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_'
}

func isSimpleJSONSubcolumnMemberStart(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value == '_'
}

func (materializer *JSONSubcolumnAutoMaterializer) newEntryLocked(logical jsonSubcolumnAutoLogicalKey, generation uint64) *jsonSubcolumnAutoEntry {
	if len(materializer.entries) >= materializer.maxEntries {
		var victimKey jsonSubcolumnAutoLogicalKey
		var victim *jsonSubcolumnAutoEntry
		for candidateKey, candidate := range materializer.entries {
			if victim == nil || candidate.sequence < victim.sequence {
				victimKey, victim = candidateKey, candidate
			}
		}
		if victim != nil {
			materializer.removeEntryLocked(victimKey, victim)
			materializer.evictions++
		}
	}
	materializer.sequence++
	if materializer.sequence == 0 {
		materializer.sequence = 1
	}
	entry := &jsonSubcolumnAutoEntry{generation: generation, sequence: materializer.sequence}
	materializer.entries[logical] = entry
	return entry
}

func (materializer *JSONSubcolumnAutoMaterializer) removeEntryLocked(logical jsonSubcolumnAutoLogicalKey, entry *jsonSubcolumnAutoEntry) {
	if current := materializer.entries[logical]; current != entry {
		return
	}
	delete(materializer.entries, logical)
	if materializer.retainedBytes >= entry.retainedBytes {
		materializer.retainedBytes -= entry.retainedBytes
	} else {
		materializer.retainedBytes = 0
	}
}

func columnarJSONSubcolumnRetainedBytes(column ColumnarJSONSubcolumn) uint64 {
	var retained uint64
	add := func(value uint64) {
		if ^uint64(0)-retained < value {
			retained = ^uint64(0)
			return
		}
		retained += value
	}
	add(uint64(len(column.Int64)) * 8)
	add(uint64(len(column.Float64)) * 8)
	add(uint64(len(column.Strings)) * uint64(unsafe.Sizeof("")))
	for _, value := range column.Strings {
		add(uint64(len(value)))
	}
	add(uint64(len(column.BoolBits)))
	add(uint64(len(column.JSONData)))
	add(uint64(len(column.JSONOffsets)) * uint64(unsafe.Sizeof(uint32(0))))
	add(uint64(len(column.Present)))
	add(uint64(len(column.Validity)))
	return retained
}
