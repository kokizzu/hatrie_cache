package hatCache

import "fmt"

type sqlJSONPartialIndex struct {
	sqlJSONIndexState
	field          string
	conditionField string
	conditionKey   string
	rows           map[string][]SQLRow
}

// CreateSQLJSONPartialIndex configures an opt-in equality index for field that
// retains only rows whose conditionField equals conditionValue. It is used for
// a matching two-field equality conjunction and otherwise the normal planner
// path remains in effect.
func (ht *HatTrie) CreateSQLJSONPartialIndex(key, field, conditionField string, conditionValue interface{}) error {
	if ht == nil || key == "" || field == "" || conditionField == "" || field == conditionField {
		return fmt.Errorf("SQL partial JSON index requires distinct cache, lookup, and condition fields")
	}
	conditionKey, ok := sqlIndexValueKey(conditionValue)
	if !ok {
		return fmt.Errorf("SQL partial JSON index condition value is not indexable")
	}
	identifier := field + "\x00" + conditionField + "\x00" + conditionKey
	ht.registerSQLJSONIndexSource(key)
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONPartialIndexes == nil {
		ht.sqlJSONPartialIndexes = make(map[string]map[string]*sqlJSONPartialIndex)
	}
	if ht.sqlJSONPartialIndexes[key] == nil {
		ht.sqlJSONPartialIndexes[key] = make(map[string]*sqlJSONPartialIndex)
	}
	ht.sqlJSONPartialIndexes[key][identifier] = &sqlJSONPartialIndex{field: field, conditionField: conditionField, conditionKey: conditionKey}
	return nil
}

func refreshSQLJSONPartialIndexSource(index *sqlJSONPartialIndex, source sqlJSONSource, rows []SQLRow) error {
	if source.current(index.sqlJSONIndexState) {
		return nil
	}
	postings := make(map[string][]SQLRow)
	for _, row := range rows {
		conditionKey, ok := sqlIndexValueKey(row[index.conditionField])
		if !ok || conditionKey != index.conditionKey {
			continue
		}
		lookupKey, ok := sqlIndexValueKey(row[index.field])
		if ok {
			postings[lookupKey] = append(postings[lookupKey], row)
		}
	}
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	index.rows = postings
	return nil
}
