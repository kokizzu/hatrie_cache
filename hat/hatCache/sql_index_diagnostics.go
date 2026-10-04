package hatCache

import (
	"strings"

	"hatrie_cache/hat/hatSql"
)

// ResolveSQLIndexDiagnostics reports bounded work for an equality probe over a
// JSON index. It is deliberately separate from candidate lookup so existing
// indexed queries keep their established result and allocation behavior.
func (ht *HatTrie) ResolveSQLIndexDiagnostics(name, key, field string, value interface{}) (hatSql.SQLIndexDiagnostics, bool, error) {
	if ht == nil || name != "CACHE" {
		return hatSql.SQLIndexDiagnostics{}, false, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return hatSql.SQLIndexDiagnostics{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	typed := ht.sqlJSONTypedInt64Indexes[key][field]
	bitmap := ht.sqlJSONBitmapIndexes[key][field]
	index := ht.sqlJSONIndexes[key][field]
	if index != nil && index.multikey {
		index = nil
	}
	if typed != nil {
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			if err == errSQLJSONIndexAdmissionDenied {
				return hatSql.SQLIndexDiagnostics{}, false, nil
			}
			return hatSql.SQLIndexDiagnostics{}, false, err
		}
		refreshSQLJSONTypedInt64IndexSource(typed, field, source, snapshot.rows)
		needle, ok := sqlJSONTypedInt64Value(value)
		candidates := 0
		if ok {
			candidates = len(typed.postings[needle])
		}
		return sqlJSONEqualityDiagnostics("json_typed_int64", field, len(snapshot.rows), candidates, sqlJSONTypedInt64IndexBytes(typed)), true, nil
	}
	if bitmap != nil {
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			if err == errSQLJSONIndexAdmissionDenied {
				return hatSql.SQLIndexDiagnostics{}, false, nil
			}
			return hatSql.SQLIndexDiagnostics{}, false, err
		}
		if err := refreshSQLJSONBitmapIndexSourceRows(bitmap, field, source, snapshot.rows); err != nil {
			return hatSql.SQLIndexDiagnostics{}, false, err
		}
		candidates := 0
		if valueKey, ok := sqlIndexValueKey(value); ok {
			candidates = int(bitmap.postings[valueKey].Count())
		}
		return sqlJSONEqualityDiagnostics("json_bitmap", field, len(snapshot.rows), candidates, sqlJSONBitmapIndexBytes(bitmap)), true, nil
	}
	if index != nil {
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			if err == errSQLJSONIndexAdmissionDenied {
				return hatSql.SQLIndexDiagnostics{}, false, nil
			}
			return hatSql.SQLIndexDiagnostics{}, false, err
		}
		if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
			return hatSql.SQLIndexDiagnostics{}, false, err
		}
		rows, available := sqlJSONFieldIndexLookupRows(index, value)
		if !available {
			return hatSql.SQLIndexDiagnostics{}, false, nil
		}
		return sqlJSONEqualityDiagnostics("json_field", field, len(snapshot.rows), len(rows), sqlJSONFieldIndexBytes(index)), true, nil
	}
	skipIndex := ht.sqlJSONPathSkipIndexes[key][field]
	if skipIndex == nil {
		return hatSql.SQLIndexDiagnostics{}, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return hatSql.SQLIndexDiagnostics{}, false, nil
		}
		return hatSql.SQLIndexDiagnostics{}, false, err
	}
	if err := refreshSQLJSONPathSkipIndexSource(skipIndex, source, snapshot.rows); err != nil {
		return hatSql.SQLIndexDiagnostics{}, false, err
	}
	diagnostics, available := sqlJSONPathSkipDiagnostics(skipIndex, field, value)
	return diagnostics, available, nil
}

// ResolveSQLCompositeIndexDiagnostics reports bounded work for a configured
// composite JSON index. It is deliberately separate from candidate lookup so
// normal indexed queries keep their established result and allocation path.
func (ht *HatTrie) ResolveSQLCompositeIndexDiagnostics(name, key string, fields []string, values []interface{}) (hatSql.SQLIndexDiagnostics, bool, error) {
	if ht == nil || name != "CACHE" || len(fields) != len(values) || len(fields) < 2 {
		return hatSql.SQLIndexDiagnostics{}, false, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return hatSql.SQLIndexDiagnostics{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	for _, candidate := range ht.sqlJSONPartialIndexes[key] {
		lookup, lookupOK := sqlJSONCompositeDiagnosticValue(fields, values, candidate.field)
		condition, conditionOK := sqlJSONCompositeDiagnosticValue(fields, values, candidate.conditionField)
		conditionKey, keyed := sqlIndexValueKey(condition)
		if !lookupOK || !conditionOK || !keyed || conditionKey != candidate.conditionKey {
			continue
		}
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			if err == errSQLJSONIndexAdmissionDenied {
				return hatSql.SQLIndexDiagnostics{}, false, nil
			}
			return hatSql.SQLIndexDiagnostics{}, false, err
		}
		if err := refreshSQLJSONPartialIndexSource(candidate, source, snapshot.rows); err != nil {
			return hatSql.SQLIndexDiagnostics{}, false, err
		}
		lookupKey, ok := sqlIndexValueKey(lookup)
		if !ok {
			return sqlJSONEqualityDiagnostics("json_partial", candidate.field+","+candidate.conditionField, len(snapshot.rows), 0, sqlJSONPartialIndexBytes(candidate)), true, nil
		}
		return sqlJSONEqualityDiagnostics("json_partial", candidate.field+","+candidate.conditionField, len(snapshot.rows), len(candidate.rows[lookupKey]), sqlJSONPartialIndexBytes(candidate)), true, nil
	}
	var selected *sqlJSONCompositeIndex
	for _, candidate := range ht.sqlJSONCompositeIndexes[key] {
		if len(candidate.fields) <= 1 || selected != nil && len(candidate.fields) <= len(selected.fields) {
			continue
		}
		available := true
		for _, field := range candidate.fields {
			if _, ok := sqlJSONCompositeDiagnosticValue(fields, values, field); !ok {
				available = false
				break
			}
		}
		if available {
			selected = candidate
		}
	}
	if selected == nil {
		return hatSql.SQLIndexDiagnostics{}, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return hatSql.SQLIndexDiagnostics{}, false, nil
		}
		return hatSql.SQLIndexDiagnostics{}, false, err
	}
	if err := refreshSQLJSONCompositeIndexSourceRows(selected, source, snapshot.rows); err != nil {
		return hatSql.SQLIndexDiagnostics{}, false, err
	}
	lookup := make([]interface{}, len(selected.fields))
	for index, field := range selected.fields {
		value, ok := sqlJSONCompositeDiagnosticValue(fields, values, field)
		if !ok {
			return hatSql.SQLIndexDiagnostics{}, false, nil
		}
		lookup[index] = value
	}
	valueKey, ok := sqlJSONCompositeIndexValueKey(lookup)
	if !ok {
		return sqlJSONEqualityDiagnostics("json_composite", strings.Join(selected.fields, ","), len(snapshot.rows), 0, sqlJSONCompositeIndexBytes(selected)), true, nil
	}
	return sqlJSONEqualityDiagnostics("json_composite", strings.Join(selected.fields, ","), len(snapshot.rows), len(selected.rows[valueKey]), sqlJSONCompositeIndexBytes(selected)), true, nil
}

func sqlJSONCompositeDiagnosticValue(fields []string, values []interface{}, field string) (interface{}, bool) {
	for index, candidate := range fields {
		if candidate == field {
			return values[index], true
		}
	}
	return nil, false
}

func sqlJSONEqualityDiagnostics(kind, field string, totalRows, candidateRows, indexBytes int) hatSql.SQLIndexDiagnostics {
	if totalRows < 0 {
		totalRows = 0
	}
	if candidateRows < 0 {
		candidateRows = 0
	}
	if candidateRows > totalRows {
		candidateRows = totalRows
	}
	return hatSql.SQLIndexDiagnostics{
		Kind:          kind,
		Field:         field,
		IndexBytes:    indexBytes,
		TotalRows:     totalRows,
		CandidateRows: candidateRows,
		SkippedRows:   totalRows - candidateRows,
	}
}

func sqlJSONFieldIndexBytes(index *sqlJSONFieldIndex) int {
	if index == nil {
		return 0
	}
	bytes := len(index.rows) * 16
	for key, rows := range index.rows {
		bytes += len(key) + len(rows)*8
	}
	bytes += len(index.ordered) * 24
	bytes += len(index.nulls) * 8
	if bytes == 0 && index.ready {
		return 1
	}
	return bytes
}

func sqlJSONTypedInt64IndexBytes(index *sqlJSONTypedInt64Index) int {
	if index == nil {
		return 0
	}
	bytes := len(index.postings) * 16
	for _, posting := range index.postings {
		bytes += len(posting) * 4
	}
	bytes += len(index.rows) * 8
	bytes += len(index.ordered) * 16
	bytes += len(index.nulls) * 4
	if bytes == 0 && index.ready {
		return 1
	}
	return bytes
}

func sqlJSONBitmapIndexBytes(index *sqlJSONBitmapIndex) int {
	if index == nil {
		return 0
	}
	bytes := len(index.postings) * 16
	for _, posting := range index.postings {
		bytes += int(posting.EncodedSize())
	}
	bytes += len(index.rows) * 8
	bytes += len(index.nulls) * 8
	if bytes == 0 && index.ready {
		return 1
	}
	return bytes
}

func sqlJSONCompositeIndexBytes(index *sqlJSONCompositeIndex) int {
	if index == nil {
		return 0
	}
	bytes := len(index.fields) * 16
	bytes += len(index.rows) * 16
	for key, rows := range index.rows {
		bytes += len(key) + len(rows)*8
	}
	if bytes == 0 {
		return 1
	}
	return bytes
}

func sqlJSONPartialIndexBytes(index *sqlJSONPartialIndex) int {
	if index == nil {
		return 0
	}
	bytes := len(index.rows) * 16
	for key, rows := range index.rows {
		bytes += len(key) + len(rows)*8
	}
	if bytes == 0 {
		return 1
	}
	return bytes
}
