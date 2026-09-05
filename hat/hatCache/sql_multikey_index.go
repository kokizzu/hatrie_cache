package hatCache

import (
	"fmt"

	"hatrie_cache/hat/hatSql"
)

// CreateSQLJSONMultikeyIndex configures an opt-in membership index for a JSON
// array field. One source row is stored in at most one posting per distinct
// array element; scalar JSON indexes remain unchanged.
func (ht *HatTrie) CreateSQLJSONMultikeyIndex(key, field string) error {
	if ht == nil || key == "" || field == "" {
		return fmt.Errorf("SQL JSON multikey index requires a cache key and field")
	}
	ht.registerSQLJSONIndexSource(key)
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONIndexes == nil {
		ht.sqlJSONIndexes = map[string]map[string]*sqlJSONFieldIndex{}
	}
	if ht.sqlJSONIndexes[key] == nil {
		ht.sqlJSONIndexes[key] = map[string]*sqlJSONFieldIndex{}
	}
	ht.sqlJSONIndexes[key][field] = &sqlJSONFieldIndex{multikey: true}
	return nil
}

// ResolveSQLMultikeySource returns candidates whose array field contains value.
// The SQL executor rechecks ARRAY_CONTAINS, so this method is safe even when a
// caller supplies rows with mixed or unusual JSON values.
func (ht *HatTrie) ResolveSQLMultikeySource(name, key, field string, value interface{}) ([]SQLRow, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	if name != "CACHE" {
		return nil, false, nil
	}
	ht.sqlIndexMu.Lock()
	index := ht.sqlJSONIndexes[key][field]
	ht.sqlIndexMu.Unlock()
	if index == nil || !index.multikey {
		return nil, false, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index = ht.sqlJSONIndexes[key][field]
	if index == nil || !index.multikey {
		return nil, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := refreshSQLJSONMultikeyIndexSourceRows(index, field, source, snapshot.rows); err != nil {
		return nil, false, err
	}
	valueKey, ok := sqlJSONMultikeyLookupKey(index, value)
	if !ok {
		return []SQLRow{}, true, nil
	}
	if index.comparisonRows != nil {
		return hatSql.CloneRows(index.comparisonRows[valueKey]), true, nil
	}
	return hatSql.CloneRows(index.rows[valueKey]), true, nil
}

func refreshSQLJSONMultikeyIndexSourceRows(index *sqlJSONFieldIndex, field string, source sqlJSONSource, rows []SQLRow) error {
	if source.current(index.sqlJSONIndexState) {
		return nil
	}
	postings := make(map[string][]SQLRow)
	comparisonPostings := make(map[string][]SQLRow)
	nulls := make([]SQLRow, 0)
	indexedRows := 0
	hasString := false
	hasNonString := false
	for _, row := range rows {
		value, exists, err := sqlJSONIndexRowValue(row, field)
		if err != nil {
			return err
		}
		elements, ok := sqlJSONMultikeyElements(value, exists)
		if !ok {
			nulls = append(nulls, row)
			continue
		}
		seen := make(map[string]struct{}, len(elements))
		comparisonSeen := make(map[string]struct{}, len(elements))
		indexed := false
		for _, element := range elements {
			valueKey, ok := sqlIndexValueKey(element)
			if !ok {
				continue
			}
			if _, isString := element.(string); isString {
				hasString = true
			} else {
				hasNonString = true
			}
			if _, duplicate := seen[valueKey]; !duplicate {
				seen[valueKey] = struct{}{}
				postings[valueKey] = append(postings[valueKey], row)
				indexed = true
			}
			comparisonKey := sqlJSONMultikeyComparisonKey(element)
			if _, duplicate := comparisonSeen[comparisonKey]; duplicate {
				continue
			}
			comparisonSeen[comparisonKey] = struct{}{}
			comparisonPostings[comparisonKey] = append(comparisonPostings[comparisonKey], row)
		}
		if indexed {
			indexedRows++
		} else {
			nulls = append(nulls, row)
		}
	}
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	index.rows = postings
	if !hasString || !hasNonString {
		comparisonPostings = nil
	}
	index.comparisonRows = comparisonPostings
	index.ordered = nil
	index.nulls = nulls
	index.stringOnly = false
	index.rowCount = len(rows)
	index.indexedRowCount = indexedRows
	return nil
}

func sqlJSONMultikeyElements(value interface{}, exists bool) ([]interface{}, bool) {
	if !exists || value == nil {
		return nil, false
	}
	elements, ok := value.([]interface{})
	return elements, ok
}

func sqlJSONMultikeyLookupKey(index *sqlJSONFieldIndex, value interface{}) (string, bool) {
	if index.comparisonRows != nil {
		if value == nil {
			return "", false
		}
		return sqlJSONMultikeyComparisonKey(value), true
	}
	return sqlIndexValueKey(value)
}

func sqlJSONMultikeyComparisonKey(value interface{}) string {
	return "comparison:" + fmt.Sprint(value)
}
