package hatSchema

import (
	"errors"
	"fmt"
	"hatrie_cache/hat/hatSql"
	"sort"
	"strings"
	"sync"
)

var (
	ErrMaterializedSourceNil                                 = errors.New("hatSchema: materialized source is nil")
	ErrMaterializedSourceColumnRequired                      = errors.New("hatSchema: materialized source index column is required")
	ErrMaterializedSourceColumnUnknown                       = errors.New("hatSchema: materialized source index column is unknown")
	ErrMaterializedSourceCoveringFieldsRequired              = errors.New("hatSchema: materialized source covering fields are required")
	ErrMaterializedSourceFunctionalIndexNameRequired         = errors.New("hatSchema: materialized source functional index name is required")
	ErrMaterializedSourceFunctionalIndexDependenciesRequired = errors.New("hatSchema: materialized source functional index dependencies are required")
	ErrMaterializedSourceFunctionalIndexEvaluatorRequired    = errors.New("hatSchema: materialized source functional index evaluator is required")
	ErrMaterializedSourceFunctionalIndexNameConflict         = errors.New("hatSchema: materialized source functional index name conflicts with an existing index")
	ErrMaterializedSourceUniqueIndexViolation                = errors.New("hatSchema: materialized source unique index violation")
)

type GeneratedValue func(Row) (interface{}, error)

// FunctionalIndexEvaluator derives the lookup value for one materialized row.
// Evaluators should be deterministic and read-only; the source passes a row
// copy so an accidental mutation cannot alter stored data.
type FunctionalIndexEvaluator func(Row) (interface{}, error)

type DerivedColumn struct {
	Name      string
	Default   interface{}
	Identity  bool
	Sequence  string
	Generated GeneratedValue
	Indexed   bool
}

type SQLResolverAdapter struct {
	Base    hatSql.SourceResolver
	Sources map[string]*MaterializedSource
}

func (adapter SQLResolverAdapter) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if strings.EqualFold(name, "CACHE") {
		if source := adapter.Sources[strings.ToLower(key)]; source != nil {
			return sqlRows(source.Rows()), nil
		}
	}
	if adapter.Base == nil {
		return nil, nil
	}
	return adapter.Base.ResolveSQLSource(name, key)
}

func (adapter SQLResolverAdapter) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	if strings.EqualFold(name, "CACHE") {
		if source := adapter.Sources[strings.ToLower(key)]; source != nil {
			if !source.HasIndex(field) {
				return nil, false, nil
			}
			return sqlRows(source.Lookup(field, value)), true, nil
		}
	}
	if indexed, ok := adapter.Base.(hatSql.IndexedSourceResolver); ok {
		return indexed.ResolveSQLIndexedSource(name, key, field, value)
	}
	return nil, false, nil
}

// SQLJSONIndexStats exposes current materialized-source index distribution
// statistics to the SQL planner. The source remains the authority for rows;
// a non-CACHE source is delegated to the optional base resolver.
func (adapter SQLResolverAdapter) SQLJSONIndexStats(key string, fields ...string) (hatSql.JSONIndexStats, bool, error) {
	if source := adapter.Sources[strings.ToLower(key)]; source != nil {
		stats, available, err := source.IndexStats(fields...)
		stats.Key = key
		return stats, available, err
	}
	if provider, ok := adapter.Base.(hatSql.JSONIndexStatsResolver); ok {
		return provider.SQLJSONIndexStats(key, fields...)
	}
	return hatSql.JSONIndexStats{}, false, nil
}

// SQLJSONIndexValueEstimate exposes one exact posting-list size to the SQL
// planner without materializing rows or the full index distribution.
func (adapter SQLResolverAdapter) SQLJSONIndexValueEstimate(key, field string, value interface{}) (int, bool, bool, error) {
	if source := adapter.Sources[strings.ToLower(key)]; source != nil {
		return source.IndexValueEstimate(field, value)
	}
	if provider, ok := adapter.Base.(hatSql.IndexValueEstimator); ok {
		return provider.SQLJSONIndexValueEstimate(key, field, value)
	}
	return 0, false, false, nil
}

// ResolveSQLCoveringSource resolves a CACHE equality predicate from a
// covering index when every requested field is stored by that index.
func (adapter SQLResolverAdapter) ResolveSQLCoveringSource(name, key, field string, value interface{}, fields []string) ([]hatSql.Row, bool, error) {
	if strings.EqualFold(name, "CACHE") {
		if source := adapter.Sources[strings.ToLower(key)]; source != nil {
			rows, available := source.LookupCovering(field, value, fields)
			if !available {
				return nil, false, nil
			}
			return sqlRows(rows), true, nil
		}
	}
	if covering, ok := adapter.Base.(hatSql.CoveringIndexedSourceResolver); ok {
		return covering.ResolveSQLCoveringSource(name, key, field, value, fields)
	}
	return nil, false, nil
}

func sqlRows(rows []Row) []hatSql.Row {
	converted := make([]hatSql.Row, len(rows))
	for index, row := range rows {
		converted[index] = hatSql.Row(row)
	}
	return converted
}

// SecondaryIndexBuildReport describes one online secondary-index build.
type SecondaryIndexBuildReport struct {
	Field    string
	Rows     int
	Attempts int
}

// CoveringIndexBuildReport describes one online covering-index build.
type CoveringIndexBuildReport struct {
	Field    string
	Fields   []string
	Rows     int
	Attempts int
}

// FunctionalIndexBuildReport describes one online functional-index build.
type FunctionalIndexBuildReport struct {
	Name         string
	Dependencies []string
	Rows         int
	Attempts     int
}

type materializedCoveringIndex struct {
	fields    []string
	fieldSet  map[string]struct{}
	positions map[string][]int
	rows      map[string][]Row
}

type materializedFunctionalIndex struct {
	dependencies []string
	evaluator    FunctionalIndexEvaluator
	positions    map[string][]int
}

type MaterializedSource struct {
	mu                sync.RWMutex
	columns           []DerivedColumn
	nextID            map[string]int64
	rows              []Row
	indexes           map[string]map[string][]int
	indexedFields     map[string]struct{}
	uniqueFields      map[string]struct{}
	coveringIndexes   map[string]*materializedCoveringIndex
	functionalIndexes map[string]*materializedFunctionalIndex
	indexStatsCache   map[string]hatSql.JSONIndexStats
	generation        uint64
}

func NewMaterializedSource(columns []DerivedColumn) *MaterializedSource {
	indexedFields := make(map[string]struct{})
	for _, column := range columns {
		if column.Indexed {
			indexedFields[column.Name] = struct{}{}
		}
	}
	return &MaterializedSource{
		columns:           append([]DerivedColumn(nil), columns...),
		nextID:            map[string]int64{},
		indexes:           map[string]map[string][]int{},
		indexedFields:     indexedFields,
		uniqueFields:      map[string]struct{}{},
		coveringIndexes:   map[string]*materializedCoveringIndex{},
		functionalIndexes: map[string]*materializedFunctionalIndex{},
	}
}

func (source *MaterializedSource) Insert(row Row) (Row, error) {
	if source == nil {
		return nil, fmt.Errorf("hatSchema: materialized source is nil")
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	materialized := cloneRow(row)
	for _, column := range source.columns {
		if column.Name == "" {
			return nil, fmt.Errorf("hatSchema: derived column name is required")
		}
		if _, exists := materialized[column.Name]; !exists && column.Default != nil {
			materialized[column.Name] = column.Default
		}
		if _, exists := materialized[column.Name]; !exists && (column.Identity || column.Sequence != "") {
			sequence := column.Name
			if column.Sequence != "" {
				sequence = column.Sequence
			}
			source.nextID[sequence]++
			materialized[column.Name] = source.nextID[sequence]
		}
	}
	for _, column := range source.columns {
		if column.Generated == nil {
			continue
		}
		value, err := column.Generated(cloneRow(materialized))
		if err != nil {
			return nil, fmt.Errorf("hatSchema: generate %q: %w", column.Name, err)
		}
		materialized[column.Name] = value
	}
	var functionalKeys map[string]string
	if len(source.functionalIndexes) > 0 {
		functionalKeys = make(map[string]string, len(source.functionalIndexes))
		for name, index := range source.functionalIndexes {
			if index == nil || index.evaluator == nil {
				continue
			}
			value, err := index.evaluator(cloneRow(materialized))
			if err != nil {
				return nil, fmt.Errorf("hatSchema: evaluate functional index %q: %w", name, err)
			}
			functionalKeys[name] = materializedIndexKey(value)
		}
	}
	for field := range source.uniqueFields {
		value := materialized[field]
		if value == nil {
			continue
		}
		key := materializedIndexKey(value)
		if len(source.indexes[field][key]) > 0 {
			return nil, fmt.Errorf("%w: field %q", ErrMaterializedSourceUniqueIndexViolation, field)
		}
	}
	position := len(source.rows)
	source.rows = append(source.rows, cloneRow(materialized))
	for field := range source.indexedFields {
		if source.indexes[field] == nil {
			source.indexes[field] = map[string][]int{}
		}
		key := materializedIndexKey(materialized[field])
		source.indexes[field][key] = append(source.indexes[field][key], position)
	}
	for field, index := range source.coveringIndexes {
		if index == nil {
			continue
		}
		key := materializedIndexKey(materialized[field])
		index.positions[key] = append(index.positions[key], position)
		index.rows[key] = append(index.rows[key], projectMaterializedRow(materialized, index.fields))
	}
	for name, index := range source.functionalIndexes {
		if index == nil {
			continue
		}
		key := functionalKeys[name]
		index.positions[key] = append(index.positions[key], position)
	}
	source.generation++
	source.indexStatsCache = nil
	return cloneRow(materialized), nil
}

// HasIndex reports whether field has a maintained equality index.
func (source *MaterializedSource) HasIndex(field string) bool {
	if source == nil {
		return false
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return false
	}
	source.mu.RLock()
	_, indexed := source.indexedFields[field]
	if !indexed {
		_, indexed = source.coveringIndexes[field]
	}
	if !indexed {
		_, indexed = source.functionalIndexes[field]
	}
	source.mu.RUnlock()
	return indexed
}

// IndexStats returns current cardinality and posting-distribution statistics
// for one maintained equality index. The bounded result is cached until the
// next row or index mutation.
func (source *MaterializedSource) IndexStats(fields ...string) (hatSql.JSONIndexStats, bool, error) {
	if source == nil || len(fields) != 1 {
		return hatSql.JSONIndexStats{}, false, nil
	}
	field := strings.TrimSpace(fields[0])
	if field == "" {
		return hatSql.JSONIndexStats{}, false, nil
	}
	source.mu.RLock()
	if cached, ok := source.indexStatsCache[field]; ok {
		source.mu.RUnlock()
		return cloneMaterializedIndexStats(cached), true, nil
	}
	generation := source.generation
	stats, available := source.indexStatsLocked(field)
	source.mu.RUnlock()
	if !available {
		return hatSql.JSONIndexStats{}, false, nil
	}

	source.mu.Lock()
	if source.generation == generation {
		if source.indexStatsCache == nil {
			source.indexStatsCache = make(map[string]hatSql.JSONIndexStats)
		}
		source.indexStatsCache[field] = cloneMaterializedIndexStats(stats)
	}
	source.mu.Unlock()
	return stats, true, nil
}

// IndexValueEstimate returns the exact current posting-list size for one
// maintained equality index without cloning matching rows.
func (source *MaterializedSource) IndexValueEstimate(field string, value interface{}) (int, bool, bool, error) {
	if source == nil {
		return 0, false, false, nil
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return 0, false, false, nil
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	var postings map[string][]int
	if _, indexed := source.indexedFields[field]; indexed {
		postings = source.indexes[field]
	} else if index := source.coveringIndexes[field]; index != nil {
		postings = index.positions
	} else if index := source.functionalIndexes[field]; index != nil {
		postings = index.positions
	} else {
		return 0, false, false, nil
	}
	return len(postings[materializedIndexKey(value)]), true, true, nil
}

func (source *MaterializedSource) indexStatsLocked(field string) (hatSql.JSONIndexStats, bool) {
	var postings map[string][]int
	includeNull := false
	if _, indexed := source.indexedFields[field]; indexed {
		postings = source.indexes[field]
		includeNull = true
	} else if index := source.coveringIndexes[field]; index != nil {
		postings = index.positions
		includeNull = true
	} else if index := source.functionalIndexes[field]; index != nil {
		postings = index.positions
	} else {
		return hatSql.JSONIndexStats{}, false
	}

	stats := hatSql.JSONIndexStats{Fields: []string{field}, Rows: len(source.rows)}
	nullRows := 0
	if includeNull {
		for _, row := range source.rows {
			if row[field] == nil {
				nullRows++
			}
		}
		stats.NullRows = nullRows
	}
	nullKey := materializedIndexKey(nil)
	frequencies := make(map[int]int)
	for key, positions := range postings {
		if includeNull && key == nullKey {
			continue
		}
		count := len(positions)
		if count == 0 {
			continue
		}
		stats.DistinctKeys++
		if stats.MinRowsPerKey == 0 || count < stats.MinRowsPerKey {
			stats.MinRowsPerKey = count
		}
		if count > stats.MaxRowsPerKey {
			stats.MaxRowsPerKey = count
		}
		frequencies[count]++
	}
	if stats.DistinctKeys > 0 {
		stats.AverageRowsPerKey = float64(len(source.rows)-nullRows) / float64(stats.DistinctKeys)
		counts := make([]int, 0, len(frequencies))
		for count := range frequencies {
			counts = append(counts, count)
		}
		sort.Ints(counts)
		stats.FrequencyHistogram = make([]hatSql.JSONIndexFrequencyBucket, 0, len(counts))
		for _, count := range counts {
			stats.FrequencyHistogram = append(stats.FrequencyHistogram, hatSql.JSONIndexFrequencyBucket{RowsPerKey: count, DistinctKeys: frequencies[count]})
		}
	}
	return stats, true
}

func cloneMaterializedIndexStats(stats hatSql.JSONIndexStats) hatSql.JSONIndexStats {
	stats.Fields = append([]string(nil), stats.Fields...)
	stats.FrequencyHistogram = append([]hatSql.JSONIndexFrequencyBucket(nil), stats.FrequencyHistogram...)
	return stats
}

// BuildCoveringIndex builds and atomically installs an equality index that
// stores only the requested projected fields. Inserts and reads continue while
// the source is scanned, and a concurrent insert causes a bounded retry.
func (source *MaterializedSource) BuildCoveringIndex(field string, fields []string) (CoveringIndexBuildReport, error) {
	if source == nil {
		return CoveringIndexBuildReport{}, ErrMaterializedSourceNil
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return CoveringIndexBuildReport{}, ErrMaterializedSourceColumnRequired
	}
	normalizedFields, err := normalizeCoveringFields(field, fields)
	if err != nil {
		return CoveringIndexBuildReport{}, err
	}
	report := CoveringIndexBuildReport{Field: field, Fields: append([]string(nil), normalizedFields...)}
	for {
		source.mu.RLock()
		if !source.hasColumnLocked(field) {
			source.mu.RUnlock()
			return CoveringIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, field)
		}
		for _, projectedField := range normalizedFields {
			if !source.hasColumnLocked(projectedField) {
				source.mu.RUnlock()
				return CoveringIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, projectedField)
			}
		}
		generation := source.generation
		rows := append([]Row(nil), source.rows...)
		source.mu.RUnlock()

		index := &materializedCoveringIndex{
			fields:    append([]string(nil), normalizedFields...),
			fieldSet:  make(map[string]struct{}, len(normalizedFields)),
			positions: make(map[string][]int, len(rows)),
			rows:      make(map[string][]Row, len(rows)),
		}
		for _, projectedField := range normalizedFields {
			index.fieldSet[projectedField] = struct{}{}
		}
		for position, row := range rows {
			key := materializedIndexKey(row[field])
			index.positions[key] = append(index.positions[key], position)
			index.rows[key] = append(index.rows[key], projectMaterializedRow(row, normalizedFields))
		}
		report.Attempts++

		source.mu.Lock()
		if source.generation != generation {
			source.mu.Unlock()
			continue
		}
		if source.coveringIndexes == nil {
			source.coveringIndexes = make(map[string]*materializedCoveringIndex)
		}
		source.coveringIndexes[field] = index
		source.indexStatsCache = nil
		source.mu.Unlock()
		report.Rows = len(rows)
		return report, nil
	}
}

// HasCoveringIndex reports whether an installed covering index contains every
// requested field. Field order does not matter.
func (source *MaterializedSource) HasCoveringIndex(field string, fields []string) bool {
	if source == nil {
		return false
	}
	field = strings.TrimSpace(field)
	if field == "" || len(fields) == 0 {
		return false
	}
	source.mu.RLock()
	index := source.coveringIndexes[field]
	available := coveringIndexCovers(index, fields)
	source.mu.RUnlock()
	return available
}

// BuildSecondaryIndex builds and atomically installs an equality index while
// allowing inserts and reads to continue. The builder snapshots row headers,
// performs the expensive map construction outside the lock, and retries when
// a concurrent insert changes the source generation before publication.
func (source *MaterializedSource) BuildSecondaryIndex(field string) (SecondaryIndexBuildReport, error) {
	if source == nil {
		return SecondaryIndexBuildReport{}, ErrMaterializedSourceNil
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return SecondaryIndexBuildReport{}, ErrMaterializedSourceColumnRequired
	}
	report := SecondaryIndexBuildReport{Field: field}
	for {
		source.mu.RLock()
		if !source.hasColumnLocked(field) {
			source.mu.RUnlock()
			return SecondaryIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, field)
		}
		generation := source.generation
		rows := append([]Row(nil), source.rows...)
		source.mu.RUnlock()

		index := make(map[string][]int, len(rows))
		for position, row := range rows {
			key := materializedIndexKey(row[field])
			index[key] = append(index[key], position)
		}
		report.Attempts++

		source.mu.Lock()
		if source.generation != generation {
			source.mu.Unlock()
			continue
		}
		if source.indexes == nil {
			source.indexes = make(map[string]map[string][]int)
		}
		if source.indexedFields == nil {
			source.indexedFields = make(map[string]struct{})
		}
		source.indexes[field] = index
		source.indexedFields[field] = struct{}{}
		source.indexStatsCache = nil
		source.mu.Unlock()
		report.Rows = len(rows)
		return report, nil
	}
}

// BuildUniqueIndex validates and atomically installs a maintained equality
// index that rejects duplicate non-NULL values on later inserts. Existing
// rows are scanned outside the source lock; a concurrent insert causes a
// generation-checked retry, and duplicate validation never publishes a
// partial index.
func (source *MaterializedSource) BuildUniqueIndex(field string) (SecondaryIndexBuildReport, error) {
	if source == nil {
		return SecondaryIndexBuildReport{}, ErrMaterializedSourceNil
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return SecondaryIndexBuildReport{}, ErrMaterializedSourceColumnRequired
	}
	report := SecondaryIndexBuildReport{Field: field}
	for {
		source.mu.RLock()
		if !source.hasColumnLocked(field) {
			source.mu.RUnlock()
			return SecondaryIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, field)
		}
		generation := source.generation
		rows := append([]Row(nil), source.rows...)
		source.mu.RUnlock()

		index := make(map[string][]int, len(rows))
		for position, row := range rows {
			key := materializedIndexKey(row[field])
			if row[field] != nil && len(index[key]) > 0 {
				return SecondaryIndexBuildReport{}, fmt.Errorf("%w: field %q", ErrMaterializedSourceUniqueIndexViolation, field)
			}
			index[key] = append(index[key], position)
		}
		report.Attempts++

		source.mu.Lock()
		if source.generation != generation {
			source.mu.Unlock()
			continue
		}
		if source.indexes == nil {
			source.indexes = make(map[string]map[string][]int)
		}
		if source.indexedFields == nil {
			source.indexedFields = make(map[string]struct{})
		}
		if source.uniqueFields == nil {
			source.uniqueFields = make(map[string]struct{})
		}
		source.indexes[field] = index
		source.indexedFields[field] = struct{}{}
		source.uniqueFields[field] = struct{}{}
		source.indexStatsCache = nil
		source.mu.Unlock()
		report.Rows = len(rows)
		return report, nil
	}
}

// BuildFunctionalIndex builds and atomically installs an equality index over a
// deterministic expression. The evaluator runs outside the source lock while
// the source is scanned; a concurrent insert causes a generation-checked retry.
// The index is maintained for subsequent inserts after publication.
func (source *MaterializedSource) BuildFunctionalIndex(name string, dependencies []string, evaluator FunctionalIndexEvaluator) (FunctionalIndexBuildReport, error) {
	if source == nil {
		return FunctionalIndexBuildReport{}, ErrMaterializedSourceNil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return FunctionalIndexBuildReport{}, ErrMaterializedSourceFunctionalIndexNameRequired
	}
	if evaluator == nil {
		return FunctionalIndexBuildReport{}, ErrMaterializedSourceFunctionalIndexEvaluatorRequired
	}
	normalizedDependencies, err := normalizeFunctionalIndexDependencies(dependencies)
	if err != nil {
		return FunctionalIndexBuildReport{}, err
	}
	report := FunctionalIndexBuildReport{Name: name, Dependencies: append([]string(nil), normalizedDependencies...)}
	for {
		source.mu.RLock()
		if source.hasColumnLocked(name) {
			source.mu.RUnlock()
			return FunctionalIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceFunctionalIndexNameConflict, name)
		}
		if _, exists := source.indexedFields[name]; exists {
			source.mu.RUnlock()
			return FunctionalIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceFunctionalIndexNameConflict, name)
		}
		if _, exists := source.coveringIndexes[name]; exists {
			source.mu.RUnlock()
			return FunctionalIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceFunctionalIndexNameConflict, name)
		}
		for _, dependency := range normalizedDependencies {
			if !source.hasColumnLocked(dependency) {
				source.mu.RUnlock()
				return FunctionalIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, dependency)
			}
		}
		generation := source.generation
		rows := append([]Row(nil), source.rows...)
		source.mu.RUnlock()

		positions := make(map[string][]int, len(rows))
		for position, row := range rows {
			value, evaluateErr := evaluator(cloneRow(row))
			if evaluateErr != nil {
				return FunctionalIndexBuildReport{}, fmt.Errorf("hatSchema: evaluate functional index %q at row %d: %w", name, position, evaluateErr)
			}
			key := materializedIndexKey(value)
			positions[key] = append(positions[key], position)
		}
		report.Attempts++

		source.mu.Lock()
		if source.generation != generation {
			source.mu.Unlock()
			continue
		}
		if source.functionalIndexes == nil {
			source.functionalIndexes = make(map[string]*materializedFunctionalIndex)
		}
		source.functionalIndexes[name] = &materializedFunctionalIndex{
			dependencies: append([]string(nil), normalizedDependencies...),
			evaluator:    evaluator,
			positions:    positions,
		}
		source.indexStatsCache = nil
		source.mu.Unlock()
		report.Rows = len(rows)
		return report, nil
	}
}

func (source *MaterializedSource) Lookup(field string, value interface{}) []Row {
	if source == nil {
		return nil
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	key := materializedIndexKey(value)
	positions := source.indexes[field][key]
	if positions == nil {
		if index := source.coveringIndexes[field]; index != nil {
			positions = index.positions[key]
		}
	}
	if positions == nil {
		if index := source.functionalIndexes[field]; index != nil {
			positions = index.positions[key]
		}
	}
	rows := make([]Row, 0, len(positions))
	for _, position := range positions {
		rows = append(rows, cloneRow(source.rows[position]))
	}
	return rows
}

// LookupCovering returns projected row copies when the installed covering
// index contains every requested field. The boolean is false when callers
// should fall back to another resolver or a full scan.
func (source *MaterializedSource) LookupCovering(field string, value interface{}, fields []string) ([]Row, bool) {
	if source == nil {
		return nil, false
	}
	field = strings.TrimSpace(field)
	if field == "" || len(fields) == 0 {
		return nil, false
	}
	normalizedFields, err := normalizeCoveringFields(field, fields)
	if err != nil {
		return nil, false
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	index := source.coveringIndexes[field]
	if !coveringIndexCovers(index, normalizedFields) {
		return nil, false
	}
	projected := index.rows[materializedIndexKey(value)]
	rows := make([]Row, len(projected))
	for position, row := range projected {
		rows[position] = projectMaterializedRow(row, normalizedFields)
	}
	return rows, true
}

func (source *MaterializedSource) hasColumnLocked(field string) bool {
	for _, column := range source.columns {
		if column.Name == field {
			return true
		}
	}
	return false
}

func materializedIndexKey(value interface{}) string {
	return fmt.Sprintf("%T:%v", value, value)
}

func normalizeCoveringFields(indexField string, fields []string) ([]string, error) {
	if len(fields) == 0 {
		return nil, ErrMaterializedSourceCoveringFieldsRequired
	}
	normalized := make([]string, 0, len(fields)+1)
	seen := make(map[string]struct{}, len(fields)+1)
	add := func(field string) error {
		field = strings.TrimSpace(field)
		if field == "" {
			return ErrMaterializedSourceCoveringFieldsRequired
		}
		if _, exists := seen[field]; exists {
			return nil
		}
		seen[field] = struct{}{}
		normalized = append(normalized, field)
		return nil
	}
	if err := add(indexField); err != nil {
		return nil, err
	}
	for _, field := range fields {
		if err := add(field); err != nil {
			return nil, err
		}
	}
	return normalized, nil
}

func normalizeFunctionalIndexDependencies(dependencies []string) ([]string, error) {
	if len(dependencies) == 0 {
		return nil, ErrMaterializedSourceFunctionalIndexDependenciesRequired
	}
	normalized := make([]string, 0, len(dependencies))
	seen := make(map[string]struct{}, len(dependencies))
	for _, dependency := range dependencies {
		dependency = strings.TrimSpace(dependency)
		if dependency == "" {
			return nil, ErrMaterializedSourceFunctionalIndexDependenciesRequired
		}
		if _, exists := seen[dependency]; exists {
			continue
		}
		seen[dependency] = struct{}{}
		normalized = append(normalized, dependency)
	}
	if len(normalized) == 0 {
		return nil, ErrMaterializedSourceFunctionalIndexDependenciesRequired
	}
	return normalized, nil
}

func coveringIndexCovers(index *materializedCoveringIndex, fields []string) bool {
	if index == nil || len(fields) == 0 {
		return false
	}
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			return false
		}
		if _, exists := index.fieldSet[field]; !exists {
			return false
		}
	}
	return true
}

func projectMaterializedRow(row Row, fields []string) Row {
	projected := make(Row, len(fields))
	for _, field := range fields {
		projected[field] = row[field]
	}
	return projected
}

func (source *MaterializedSource) Rows() []Row {
	if source == nil {
		return nil
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	rows := make([]Row, len(source.rows))
	for index, row := range source.rows {
		rows[index] = cloneRow(row)
	}
	return rows
}

func cloneRow(row Row) Row {
	copy := make(Row, len(row))
	for key, value := range row {
		copy[key] = value
	}
	return copy
}
