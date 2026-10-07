package hatCache

import (
	"fmt"
	"strings"

	"hatrie_cache/hat/hatSql"
)

type sqlJSONUpperIndex struct {
	sqlJSONIndexState
	rows     map[string][]SQLRow
	complete bool
}

// CreateSQLJSONUpperIndex configures an opt-in equality index for
// UPPER(field). The index is used only when every present non-null value is a
// string; otherwise the query executor retains its normal scan path so SQL
// type errors remain observable.
func (ht *HatTrie) CreateSQLJSONUpperIndex(key, field string) error {
	if ht == nil || key == "" || field == "" {
		return fmt.Errorf("SQL JSON upper index requires a cache key and field")
	}
	ht.registerSQLJSONIndexSource(key)
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONUpperIndexes == nil {
		ht.sqlJSONUpperIndexes = make(map[string]map[string]*sqlJSONUpperIndex)
	}
	if ht.sqlJSONUpperIndexes[key] == nil {
		ht.sqlJSONUpperIndexes[key] = make(map[string]*sqlJSONUpperIndex)
	}
	ht.sqlJSONUpperIndexes[key][field] = &sqlJSONUpperIndex{}
	return nil
}

func (ht *HatTrie) resolveSQLJSONUpperIndexedSource(key, field string, value interface{}) ([]SQLRow, bool, error) {
	text, ok := value.(string)
	if !ok {
		return nil, false, nil
	}
	text = strings.ToUpper(text)
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONUpperIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return nil, false, nil
		}
		return nil, false, err
	}
	refreshSQLJSONUpperIndexSource(index, field, source, snapshot.rows)
	if !index.complete {
		return nil, false, nil
	}
	return hatSql.CloneRows(index.rows[text]), true, nil
}

func refreshSQLJSONUpperIndexSource(index *sqlJSONUpperIndex, field string, source sqlJSONSource, rows []SQLRow) {
	if source.current(index.sqlJSONIndexState) {
		return
	}
	postings := make(map[string][]SQLRow)
	complete := true
	for _, row := range rows {
		value, exists := row[field]
		if !exists || value == nil {
			continue
		}
		text, ok := value.(string)
		if !ok {
			complete = false
			break
		}
		folded := strings.ToUpper(text)
		postings[folded] = append(postings[folded], row)
	}
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	index.rows, index.complete = postings, complete
}
