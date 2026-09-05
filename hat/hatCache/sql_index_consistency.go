package hatCache

import (
	"reflect"
	"sort"
	"strings"
)

// SQLJSONIndexConsistency describes the freshness and structural validity of
// one configured SQL JSON index. The checker does not refresh or repair it.
type SQLJSONIndexConsistency struct {
	Kind       string   `json:"kind"`
	Fields     []string `json:"fields,omitempty"`
	Ready      bool     `json:"ready"`
	Current    bool     `json:"current"`
	Consistent bool     `json:"consistent"`
}

// SQLJSONIndexConsistencyReport describes all configured SQL JSON indexes for
// one cache key. CheckSQLJSONIndexConsistency returns available=false when no
// index is configured for the key.
type SQLJSONIndexConsistencyReport struct {
	Key        string                    `json:"key"`
	SourceRows int                       `json:"source_rows"`
	Consistent bool                      `json:"consistent"`
	Indexes    []SQLJSONIndexConsistency `json:"indexes"`
}

// CheckSQLJSONIndexConsistency independently rebuilds temporary index values
// from the current JSON source and compares them with the live indexes. It is
// intended for diagnostics and recovery checks; it never changes index state.
// The check may decode and rebuild every configured index, so callers should
// run it outside latency-sensitive request paths.
func (ht *HatTrie) CheckSQLJSONIndexConsistency(key string) (SQLJSONIndexConsistencyReport, bool, error) {
	if ht == nil {
		return SQLJSONIndexConsistencyReport{}, false, ErrNilHatTrie
	}
	if key == "" {
		return SQLJSONIndexConsistencyReport{}, false, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return SQLJSONIndexConsistencyReport{}, false, err
	}

	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if !ht.sqlJSONIndexesConfiguredLocked(key) {
		return SQLJSONIndexConsistencyReport{}, false, nil
	}
	if !ht.sqlJSONIndexSourceAdmittedLocked(source) {
		return SQLJSONIndexConsistencyReport{}, true, errSQLJSONIndexAdmissionDenied
	}
	rows, err := sqlJSONRowsString(key, source.raw)
	if err != nil {
		return SQLJSONIndexConsistencyReport{}, true, err
	}

	entries := make([]SQLJSONIndexConsistency, 0)
	appendEntry := func(kind string, fields []string, state sqlJSONIndexState, actual, candidate interface{}, configuration ...bool) {
		configurationConsistent := len(configuration) == 0 || configuration[0]
		entries = append(entries, SQLJSONIndexConsistency{
			Kind:       kind,
			Fields:     append([]string(nil), fields...),
			Ready:      state.ready,
			Current:    source.current(state),
			Consistent: configurationConsistent && state.ready && source.current(state) && reflect.DeepEqual(actual, candidate),
		})
	}

	for field, index := range ht.sqlJSONTypedInt64Indexes[key] {
		if index == nil {
			continue
		}
		candidate := sqlJSONTypedInt64Index{}
		refreshSQLJSONTypedInt64IndexSource(&candidate, field, source, rows)
		appendEntry("typed-int64", []string{field}, index.sqlJSONIndexState, index, &candidate)
	}
	for field, index := range ht.sqlJSONIndexes[key] {
		if index == nil {
			continue
		}
		candidate := sqlJSONFieldIndex{}
		if err := refreshSQLJSONFieldIndexSourceRows(&candidate, field, source, rows); err != nil {
			return SQLJSONIndexConsistencyReport{}, true, err
		}
		appendEntry("field", []string{field}, index.sqlJSONIndexState, index, &candidate)
	}
	for field, index := range ht.sqlJSONLowerIndexes[key] {
		if index == nil {
			continue
		}
		candidate := sqlJSONLowerIndex{}
		refreshSQLJSONLowerIndexSource(&candidate, field, source, rows)
		appendEntry("lower", []string{field}, index.sqlJSONIndexState, index, &candidate)
	}
	for field, index := range ht.sqlJSONBitmapIndexes[key] {
		if index == nil {
			continue
		}
		candidate := sqlJSONBitmapIndex{}
		if err := refreshSQLJSONBitmapIndexSourceRows(&candidate, field, source, rows); err != nil {
			return SQLJSONIndexConsistencyReport{}, true, err
		}
		appendEntry("bitmap", []string{field}, index.sqlJSONIndexState, index, &candidate)
	}
	for path, index := range ht.sqlJSONPathSkipIndexes[key] {
		if index == nil {
			continue
		}
		candidate := sqlJSONPathSkipIndex{
			path:            path,
			rowsPerSegment:  index.rowsPerSegment,
			bitsPerSegment:  index.bitsPerSegment,
			wordsPerSegment: index.wordsPerSegment,
		}
		if err := refreshSQLJSONPathSkipIndexSource(&candidate, source, rows); err != nil {
			return SQLJSONIndexConsistencyReport{}, true, err
		}
		appendEntry("path-skip", []string{path}, index.sqlJSONIndexState, index, &candidate, index.path == path)
	}
	for field, index := range ht.sqlJSONCoveringIndexes[key] {
		if index == nil {
			continue
		}
		candidate := sqlJSONCoveringIndex{columns: cloneSQLJSONIndexColumns(index.columns)}
		if err := refreshSQLJSONCoveringIndexSource(&candidate, key, field, source); err != nil {
			return SQLJSONIndexConsistencyReport{}, true, err
		}
		appendEntry("covering", []string{field}, index.sqlJSONIndexState, index, &candidate)
	}
	for field, index := range ht.sqlJSONTextIndexes[key] {
		if index == nil {
			continue
		}
		candidate := sqlJSONTextIndex{}
		if err := refreshSQLJSONTextIndexSourceRows(&candidate, field, source, rows); err != nil {
			return SQLJSONIndexConsistencyReport{}, true, err
		}
		appendEntry("text", []string{field}, index.sqlJSONIndexState, index, &candidate)
	}
	for identifier, index := range ht.sqlJSONCompositeIndexes[key] {
		if index == nil {
			continue
		}
		candidate := sqlJSONCompositeIndex{fields: append([]string(nil), index.fields...)}
		if err := refreshSQLJSONCompositeIndexSourceRows(&candidate, source, rows); err != nil {
			return SQLJSONIndexConsistencyReport{}, true, err
		}
		fields := append([]string(nil), index.fields...)
		if len(fields) == 0 {
			fields = []string{identifier}
		}
		appendEntry("composite", fields, index.sqlJSONIndexState, index, &candidate, identifier == sqlJSONCompositeIndexIdentifier(index.fields))
	}
	for identifier, index := range ht.sqlJSONTypedInt64CompositeIndexes[key] {
		if index == nil {
			continue
		}
		candidate := sqlJSONTypedInt64CompositeIndex{fields: append([]string(nil), index.fields...)}
		if err := refreshSQLJSONTypedInt64CompositeIndexSource(&candidate, source, rows); err != nil {
			return SQLJSONIndexConsistencyReport{}, true, err
		}
		fields := append([]string(nil), index.fields...)
		if len(fields) == 0 {
			fields = []string{identifier}
		}
		appendEntry("typed-int64-composite", fields, index.sqlJSONIndexState, index, &candidate, identifier == sqlJSONCompositeIndexIdentifier(index.fields))
	}
	for identifier, index := range ht.sqlJSONPartialIndexes[key] {
		if index == nil {
			continue
		}
		candidate := sqlJSONPartialIndex{
			field:          index.field,
			conditionField: index.conditionField,
			conditionKey:   index.conditionKey,
		}
		if err := refreshSQLJSONPartialIndexSource(&candidate, source, rows); err != nil {
			return SQLJSONIndexConsistencyReport{}, true, err
		}
		fields := []string{index.field, index.conditionField}
		if index.field == "" && index.conditionField == "" {
			fields = []string{identifier}
		}
		expectedIdentifier := index.field + "\x00" + index.conditionField + "\x00" + index.conditionKey
		appendEntry("partial", fields, index.sqlJSONIndexState, index, &candidate, identifier == expectedIdentifier)
	}

	if len(entries) == 0 {
		return SQLJSONIndexConsistencyReport{}, false, nil
	}
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].Kind != entries[right].Kind {
			return entries[left].Kind < entries[right].Kind
		}
		return strings.Join(entries[left].Fields, "\x00") < strings.Join(entries[right].Fields, "\x00")
	})
	report := SQLJSONIndexConsistencyReport{
		Key:        key,
		SourceRows: len(rows),
		Consistent: true,
		Indexes:    entries,
	}
	for _, entry := range entries {
		if !entry.Consistent {
			report.Consistent = false
			break
		}
	}
	return report, true, nil
}

func cloneSQLJSONIndexColumns(columns map[string]struct{}) map[string]struct{} {
	copyColumns := make(map[string]struct{}, len(columns))
	for column := range columns {
		copyColumns[column] = struct{}{}
	}
	return copyColumns
}

func (ht *HatTrie) sqlJSONIndexesConfiguredLocked(key string) bool {
	return len(ht.sqlJSONTypedInt64Indexes[key]) > 0 ||
		len(ht.sqlJSONTypedInt64CompositeIndexes[key]) > 0 ||
		len(ht.sqlJSONIndexes[key]) > 0 ||
		len(ht.sqlJSONLowerIndexes[key]) > 0 ||
		len(ht.sqlJSONBitmapIndexes[key]) > 0 ||
		len(ht.sqlJSONPathSkipIndexes[key]) > 0 ||
		len(ht.sqlJSONCoveringIndexes[key]) > 0 ||
		len(ht.sqlJSONTextIndexes[key]) > 0 ||
		len(ht.sqlJSONCompositeIndexes[key]) > 0 ||
		len(ht.sqlJSONPartialIndexes[key]) > 0
}
