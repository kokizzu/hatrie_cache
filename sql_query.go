package hatriecache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"hatrie_cache/hat/hatSql"
)

type SQLQueryOptions = hatSql.SQLQueryOptions
type SQLPreparedQueryCache = hatSql.SQLPreparedQueryCache
type SQLPreparedQueryCacheStats = hatSql.SQLPreparedQueryCacheStats
type SQLPreparedQuery = hatSql.SQLPreparedQuery
type SQLParameterType = hatSql.ParameterType
type SQLParameterSpec = hatSql.ParameterSpec
type SQLQueryRequest = hatSql.QueryRequest
type SQLRow = hatSql.Row
type SQLQueryResult = hatSql.QueryResult
type SQLExplainStep = hatSql.ExplainStep
type SQLQueryStats = hatSql.QueryStats
type SQLQueryObserver = hatSql.QueryObserver
type SQLQueryObserverFunc = hatSql.QueryObserverFunc
type SQLQueryEvent = hatSql.QueryEvent
type SQLQueryOperator = hatSql.QueryOperator
type SQLSourceResolver = hatSql.SourceResolver
type SQLStreamSourceResolver = hatSql.StreamSourceResolver
type SQLSnapshotLocker = hatSql.SnapshotLocker
type SQLIndexedSourceResolver = hatSql.IndexedSourceResolver
type SQLRangeIndexedSourceResolver = hatSql.RangeIndexedSourceResolver
type SQLOrderedSourceResolver = hatSql.OrderedSourceResolver
type SQLOrderedStreamSourceResolver = hatSql.OrderedStreamSourceResolver
type SQLCompositeIndexedSourceResolver = hatSql.CompositeIndexedSourceResolver
type SQLJSONIndexStatsResolver = hatSql.JSONIndexStatsResolver
type SQLIndexValueEstimator = hatSql.IndexValueEstimator
type SQLJSONIndexFrequencyBucket = hatSql.JSONIndexFrequencyBucket
type SQLJSONIndexStats = hatSql.JSONIndexStats
type SQLSourceResolverFunc = hatSql.SourceResolverFunc

const (
	SQLParameterAny       = hatSql.ParameterAny
	SQLParameterText      = hatSql.ParameterText
	SQLParameterNumber    = hatSql.ParameterNumber
	SQLParameterInteger   = hatSql.ParameterInteger
	SQLParameterDecimal   = hatSql.ParameterDecimal
	SQLParameterBoolean   = hatSql.ParameterBoolean
	SQLParameterDate      = hatSql.ParameterDate
	SQLParameterTimestamp = hatSql.ParameterTimestamp
	SQLParameterJSON      = hatSql.ParameterJSON
)

func NewSQLPreparedQueryCache(capacity int) *SQLPreparedQueryCache {
	return hatSql.NewSQLPreparedQueryCache(capacity)
}

// PrepareSQLQuery parses source once and binds a typed positional-parameter
// schema to its immutable cached template.
func PrepareSQLQuery(source string, parameters []SQLParameterSpec, cache *SQLPreparedQueryCache) (*SQLPreparedQuery, error) {
	return hatSql.PrepareSQLQuery(source, parameters, cache)
}

func ExecuteSQLQuery(source string, resolver SQLSourceResolver) (SQLQueryResult, error) {
	return hatSql.ExecuteSQLQuery(source, resolver)
}

func ExecuteSQLQueryContext(ctx context.Context, source string, resolver SQLSourceResolver, options SQLQueryOptions) (SQLQueryResult, error) {
	return hatSql.ExecuteSQLQueryContext(ctx, source, resolver, options)
}

func ExecuteSQLQueryParameters(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	return hatSql.ExecuteSQLQueryParameters(ctx, source, resolver, parameters, options)
}

func ExecuteSQLQueryRows(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, visit func([]string, SQLRow) error) error {
	return hatSql.ExecuteSQLQueryRows(ctx, source, resolver, parameters, options, visit)
}

func ExecuteSQLQueryPage(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, pageSize int, cursor string) (SQLQueryResult, error) {
	return hatSql.ExecuteSQLQueryPage(ctx, source, resolver, parameters, options, pageSize, cursor)
}

func ValidateSQLQuery(source string) error { return hatSql.ValidateSQLQuery(source) }

func parseSQLQueryWithCache(source string, parameters []interface{}, cache *SQLPreparedQueryCache) (*hatSql.ParsedQuery, error) {
	return hatSql.ParseSQLQueryWithCache(source, parameters, cache)
}

func parseSQLQueryParameters(source string, parameters []interface{}) (*hatSql.ParsedQuery, error) {
	return hatSql.ParseSQLQueryParameters(source, parameters)
}

func validateSQLQueryStreamable(query *hatSql.ParsedQuery) error {
	return hatSql.ValidateSQLQueryStreamable(query)
}

func SQLQueryColumns(query *hatSql.ParsedQuery) []string { return hatSql.SQLQueryColumns(query) }

func cloneSQLRows(rows []SQLRow) []SQLRow { return hatSql.CloneRows(rows) }

func sqlIndexedEqualityProbeCost(estimatedRows int) int {
	return hatSql.IndexedEqualityProbeCost(estimatedRows)
}

func sqlNumber(value interface{}) (float64, bool) { return hatSql.Number(value) }

func sqlInteger(value interface{}) (int64, bool) { return hatSql.Integer(value) }

type sqlDate = hatSql.SQLDate
type sqlDecimal = hatSql.SQLDecimal

func sqlBinaryValue(op string, left, right interface{}) interface{} {
	return hatSql.BinaryValue(op, left, right)
}

type sqlJSONFieldIndex struct {
	raw     string
	rows    map[string][]SQLRow
	ordered []sqlJSONFieldIndexEntry
	nulls   []SQLRow
}
type sqlJSONFieldIndexEntry struct {
	value interface{}
	row   SQLRow
}
type sqlJSONCompositeIndex struct {
	raw    string
	fields []string
	rows   map[string][]SQLRow
}

func (ht *HatTrie) CreateSQLJSONFieldIndex(key, field string) error {
	if ht == nil || key == "" || field == "" {
		return fmt.Errorf("SQL JSON index requires a cache key and field")
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONIndexes == nil {
		ht.sqlJSONIndexes = map[string]map[string]*sqlJSONFieldIndex{}
	}
	if ht.sqlJSONIndexes[key] == nil {
		ht.sqlJSONIndexes[key] = map[string]*sqlJSONFieldIndex{}
	}
	ht.sqlJSONIndexes[key][field] = &sqlJSONFieldIndex{}
	return nil
}

// CreateSQLJSONCompositeIndex creates an optional equality index over two or
// more JSON object fields in one CACHE value. Field order is significant and
// is retained in reported statistics and index keys.
func (ht *HatTrie) CreateSQLJSONCompositeIndex(key string, fields ...string) error {
	if ht == nil || key == "" || len(fields) < 2 {
		return fmt.Errorf("SQL composite JSON index requires a cache key and at least two fields")
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field == "" {
			return fmt.Errorf("SQL composite JSON index fields must not be empty")
		}
		if _, exists := seen[field]; exists {
			return fmt.Errorf("SQL composite JSON index field %q is repeated", field)
		}
		seen[field] = struct{}{}
	}
	copyFields := append([]string(nil), fields...)
	identifier := sqlJSONCompositeIndexIdentifier(copyFields)
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONCompositeIndexes == nil {
		ht.sqlJSONCompositeIndexes = map[string]map[string]*sqlJSONCompositeIndex{}
	}
	if ht.sqlJSONCompositeIndexes[key] == nil {
		ht.sqlJSONCompositeIndexes[key] = map[string]*sqlJSONCompositeIndex{}
	}
	ht.sqlJSONCompositeIndexes[key][identifier] = &sqlJSONCompositeIndex{fields: copyFields}
	return nil
}

func sqlJSONCompositeIndexIdentifier(fields []string) string { return strings.Join(fields, "\x00") }
func (ht *HatTrie) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	valueKey, ok := sqlIndexValueKey(value)
	if !ok {
		return []SQLRow{}, true, nil
	}
	return hatSql.CloneRows(index.rows[valueKey]), true, nil
}

// ResolveSQLCompositeIndexedSource uses the longest configured composite
// index whose fields are all present in the supplied equality predicates.
func (ht *HatTrie) ResolveSQLCompositeIndexedSource(name, key string, fields []string, values []interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" || len(fields) != len(values) || len(fields) < 2 {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	provided := make(map[string]interface{}, len(fields))
	for index, field := range fields {
		provided[field] = values[index]
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	var selected *sqlJSONCompositeIndex
	for _, candidate := range ht.sqlJSONCompositeIndexes[key] {
		if len(candidate.fields) <= 1 || selected != nil && len(candidate.fields) <= len(selected.fields) {
			continue
		}
		available := true
		for _, field := range candidate.fields {
			if _, ok := provided[field]; !ok {
				available = false
				break
			}
		}
		if available {
			selected = candidate
		}
	}
	if selected == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONCompositeIndex(selected, key, data); err != nil {
		return nil, false, err
	}
	lookup := make([]interface{}, len(selected.fields))
	for index, field := range selected.fields {
		lookup[index] = provided[field]
	}
	valueKey, ok := sqlJSONCompositeIndexValueKey(lookup)
	if !ok {
		return []SQLRow{}, true, nil
	}
	return hatSql.CloneRows(selected.rows[valueKey]), true, nil
}

// SQLJSONIndexStats returns fresh cardinality statistics for an optional
// single-field or composite JSON index.
func (ht *HatTrie) SQLJSONIndexStats(key string, fields ...string) (SQLJSONIndexStats, bool, error) {
	if ht == nil || key == "" || len(fields) == 0 {
		return SQLJSONIndexStats{}, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return SQLJSONIndexStats{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if len(fields) == 1 {
		index := ht.sqlJSONIndexes[key][fields[0]]
		if index == nil {
			return SQLJSONIndexStats{}, false, nil
		}
		if err := refreshSQLJSONFieldIndex(index, key, fields[0], data); err != nil {
			return SQLJSONIndexStats{}, false, err
		}
		return sqlJSONIndexStats(key, fields, index.rows, len(index.nulls)), true, nil
	}
	index := ht.sqlJSONCompositeIndexes[key][sqlJSONCompositeIndexIdentifier(fields)]
	if index == nil {
		return SQLJSONIndexStats{}, false, nil
	}
	if err := refreshSQLJSONCompositeIndex(index, key, data); err != nil {
		return SQLJSONIndexStats{}, false, err
	}
	return sqlJSONIndexStats(key, index.fields, index.rows), true, nil
}

// SQLJSONIndexValueEstimate returns the exact posting-list size for one value
// supplied by the caller. It does not expose or enumerate any indexed values.
// exact is false only when the value cannot be represented by this index;
// available is false when the requested field index does not exist.
func (ht *HatTrie) SQLJSONIndexValueEstimate(key, field string, value interface{}) (rows int, exact bool, available bool, err error) {
	if ht == nil || key == "" || field == "" {
		return 0, false, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return 0, false, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return 0, false, false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return 0, false, true, err
	}
	valueKey, ok := sqlIndexValueKey(value)
	if !ok {
		return 0, true, true, nil
	}
	return len(index.rows[valueKey]), true, true, nil
}

func sqlJSONIndexStats(key string, fields []string, postings map[string][]SQLRow, nullRows ...int) SQLJSONIndexStats {
	stats := SQLJSONIndexStats{Key: key, Fields: append([]string(nil), fields...), DistinctKeys: len(postings)}
	if len(nullRows) > 0 {
		stats.NullRows = nullRows[0]
	}
	frequencies := make(map[int]int, len(postings))
	for _, posting := range postings {
		count := len(posting)
		stats.Rows += count
		if stats.MinRowsPerKey == 0 || count < stats.MinRowsPerKey {
			stats.MinRowsPerKey = count
		}
		if count > stats.MaxRowsPerKey {
			stats.MaxRowsPerKey = count
		}
		frequencies[count]++
	}
	if stats.DistinctKeys == 0 {
		return stats
	}
	stats.AverageRowsPerKey = float64(stats.Rows) / float64(stats.DistinctKeys)
	counts := make([]int, 0, len(frequencies))
	for count := range frequencies {
		counts = append(counts, count)
	}
	sort.Ints(counts)
	stats.FrequencyHistogram = make([]SQLJSONIndexFrequencyBucket, 0, len(counts))
	for _, count := range counts {
		stats.FrequencyHistogram = append(stats.FrequencyHistogram, SQLJSONIndexFrequencyBucket{RowsPerKey: count, DistinctKeys: frequencies[count]})
	}
	return stats
}

// ResolveSQLIndexedRangeSource uses the ordered representation of an opt-in
// JSON field index. Missing and null fields are absent because ordinary SQL
// comparisons with NULL are unknown and therefore never pass WHERE.
func (ht *HatTrie) ResolveSQLIndexedRangeSource(name, key, field, operator string, value interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" || value == nil {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	start, end := 0, len(index.ordered)
	switch operator {
	case "<":
		end = sort.Search(len(index.ordered), func(i int) bool { return hatSql.Compare(index.ordered[i].value, value) >= 0 })
	case "<=":
		end = sort.Search(len(index.ordered), func(i int) bool { return hatSql.Compare(index.ordered[i].value, value) > 0 })
	case ">":
		start = sort.Search(len(index.ordered), func(i int) bool { return hatSql.Compare(index.ordered[i].value, value) > 0 })
	case ">=":
		start = sort.Search(len(index.ordered), func(i int) bool { return hatSql.Compare(index.ordered[i].value, value) >= 0 })
	default:
		return nil, false, nil
	}
	rows := make([]SQLRow, end-start)
	for i, entry := range index.ordered[start:end] {
		rows[i] = entry.row
	}
	return hatSql.CloneRows(rows), true, nil
}

// ResolveSQLOrderedSource returns every JSON source row in the exact order of
// one opt-in indexed field. It is used only for a compatible ORDER BY plan;
// callers outside the SQL executor may use it as an optional resolver method.
func (ht *HatTrie) ResolveSQLOrderedSource(name, key, field string, desc, nullsFirst, nullsLast bool) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	rows := make([]SQLRow, 0, len(index.ordered)+len(index.nulls))
	if desc {
		for end := len(index.ordered); end > 0; {
			start := end - 1
			for start > 0 && hatSql.Compare(index.ordered[start-1].value, index.ordered[end-1].value) == 0 {
				start--
			}
			for _, entry := range index.ordered[start:end] {
				rows = append(rows, entry.row)
			}
			end = start
		}
	} else {
		for _, entry := range index.ordered {
			rows = append(rows, entry.row)
		}
	}
	placeNullsFirst := false
	if len(index.ordered) > 0 {
		placeNullsFirst, _ = hatSql.OrderLess(desc, nullsFirst, nullsLast, nil, index.ordered[0].value)
	}
	if placeNullsFirst {
		rows = append(append([]SQLRow{}, index.nulls...), rows...)
	} else {
		rows = append(rows, index.nulls...)
	}
	return hatSql.CloneRows(rows), true, nil
}

// StreamSQLOrderedSource visits an indexed CACHE source in one-field SQL
// ORDER BY order. It captures immutable index slice headers while locked, then
// releases the index before calling visit; refreshes replace slices instead of
// mutating their backing arrays, so this remains a stable query snapshot.
func (ht *HatTrie) StreamSQLOrderedSource(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, visit func(SQLRow) error) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if visit == nil {
		return false, fmt.Errorf("SQL row callback is required")
	}
	if name != "CACHE" {
		return false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return false, err
	}
	ht.sqlIndexMu.Lock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		ht.sqlIndexMu.Unlock()
		return false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		ht.sqlIndexMu.Unlock()
		return false, err
	}
	ordered, nulls := index.ordered, index.nulls
	placeNullsFirst := false
	if len(ordered) > 0 {
		placeNullsFirst, _ = hatSql.OrderLess(desc, nullsFirst, nullsLast, nil, ordered[0].value)
	}
	ht.sqlIndexMu.Unlock()
	clone := func(row SQLRow) SQLRow {
		copy := make(SQLRow, len(row))
		for name, value := range row {
			copy[name] = value
		}
		return copy
	}
	emit := func(row SQLRow) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return visit(clone(row))
	}
	emitNulls := func() error {
		for _, row := range nulls {
			if err := emit(row); err != nil {
				return err
			}
		}
		return nil
	}
	emitOrdered := func() error {
		if desc {
			for end := len(ordered); end > 0; {
				start := end - 1
				for start > 0 && hatSql.Compare(ordered[start-1].value, ordered[end-1].value) == 0 {
					start--
				}
				for _, entry := range ordered[start:end] {
					if err := emit(entry.row); err != nil {
						return err
					}
				}
				end = start
			}
			return nil
		}
		for _, entry := range ordered {
			if err := emit(entry.row); err != nil {
				return err
			}
		}
		return nil
	}
	if placeNullsFirst {
		if err := emitNulls(); err != nil {
			return true, err
		}
		return true, emitOrdered()
	}
	if err := emitOrdered(); err != nil {
		return true, err
	}
	return true, emitNulls()
}

func refreshSQLJSONFieldIndex(index *sqlJSONFieldIndex, key, field string, data []byte) error {
	if index.raw == string(data) {
		return nil
	}
	rows, err := sqlJSONRows(key, data)
	if err != nil {
		return err
	}
	index.raw, index.rows, index.ordered, index.nulls = string(data), map[string][]SQLRow{}, nil, nil
	for _, row := range rows {
		if valueKey, ok := sqlIndexValueKey(row[field]); ok {
			index.rows[valueKey] = append(index.rows[valueKey], row)
			index.ordered = append(index.ordered, sqlJSONFieldIndexEntry{value: row[field], row: row})
		} else {
			index.nulls = append(index.nulls, row)
		}
	}
	sort.SliceStable(index.ordered, func(i, j int) bool {
		return hatSql.Compare(index.ordered[i].value, index.ordered[j].value) < 0
	})
	return nil
}

func refreshSQLJSONCompositeIndex(index *sqlJSONCompositeIndex, key string, data []byte) error {
	if index.raw == string(data) {
		return nil
	}
	rows, err := sqlJSONRows(key, data)
	if err != nil {
		return err
	}
	index.raw, index.rows = string(data), map[string][]SQLRow{}
	for _, row := range rows {
		values := make([]interface{}, len(index.fields))
		for fieldIndex, field := range index.fields {
			values[fieldIndex] = row[field]
		}
		if valueKey, ok := sqlJSONCompositeIndexValueKey(values); ok {
			index.rows[valueKey] = append(index.rows[valueKey], row)
		}
	}
	return nil
}

func sqlJSONCompositeIndexValueKey(values []interface{}) (string, bool) {
	keys := make([]string, len(values))
	for index, value := range values {
		key, ok := sqlIndexValueKey(value)
		if !ok {
			return "", false
		}
		keys[index] = key
	}
	return strings.Join(keys, "\x00"), true
}
func sqlJSONRows(key string, data []byte) ([]SQLRow, error) {
	if len(data) == 0 {
		return []SQLRow{}, nil
	}
	var rows []SQLRow
	if json.Unmarshal(data, &rows) == nil {
		return rows, nil
	}
	var row SQLRow
	if json.Unmarshal(data, &row) == nil {
		return []SQLRow{row}, nil
	}
	return nil, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
}
func sqlIndexValueKey(value interface{}) (string, bool) {
	if value == nil {
		return "", false
	}
	encoded, err := json.Marshal(value)
	return string(encoded), err == nil
}

// ResolveSQLSource exposes a stable, read-only snapshot of cache data to SQL.
// CACHE(key) requires a JSON object or array of JSON objects. KEYS returns the
// same metadata fields exposed by the monitoring entries endpoint.
func (ht *HatTrie) ResolveSQLSource(name string, key string) ([]SQLRow, error) {
	switch name {
	case "KEYS":
		entries := ht.monitoringEntries("")
		rows := make([]SQLRow, 0, len(entries))
		for _, entry := range entries {
			rows = append(rows, SQLRow{"key": entry.Key, "type": entry.Type, "ttl_ms": entry.TTLMillis, "on_disk": entry.OnDisk, "size_bytes": entry.SizeBytes, "value_preview": entry.ValuePreview})
		}
		return rows, nil
	case "CACHE":
		data, err := ht.GetBytesChecked(key)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return []SQLRow{}, nil
		}
		var array []SQLRow
		if err := json.Unmarshal(data, &array); err == nil {
			return array, nil
		}
		var object SQLRow
		if err := json.Unmarshal(data, &object); err == nil {
			return []SQLRow{object}, nil
		}
		return nil, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	default:
		return nil, fmt.Errorf("unknown SQL source %q", name)
	}
}

// StreamSQLSource visits CACHE JSON object rows incrementally. KEYS is not
// streamable yet because its monitoring metadata scan has different expiration
// maintenance semantics.
func (ht *HatTrie) StreamSQLSource(ctx context.Context, name string, key string, visit func(SQLRow) error) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if visit == nil {
		return fmt.Errorf("SQL row callback is required")
	}
	if name != "CACHE" {
		return fmt.Errorf("SQL source %q does not support row streaming", name)
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return err
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil
	}
	if !json.Valid(data) {
		return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	}
	if data[0] != '[' {
		var row SQLRow
		if err := json.Unmarshal(data, &row); err != nil {
			return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return visit(row)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '[' {
		return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	}
	for decoder.More() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var row SQLRow
		if err := decoder.Decode(&row); err != nil {
			return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	}
	return nil
}

// LockSQLSnapshot exists for resolvers that coordinate snapshot lifetime. A
// HatTrie source copies and memoizes each resolved source per query; it must not
// hold ht.mu across execution because normal source readers acquire the same
// lock and KEYS may need its exclusive maintenance path.
func (ht *HatTrie) LockSQLSnapshot() func() {
	return func() {}
}
