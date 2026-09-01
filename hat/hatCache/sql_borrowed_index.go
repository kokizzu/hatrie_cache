package hatCache

import "hatrie_cache/hat/hatSql"

// BorrowSQLIndexedSource exposes immutable postings from an ordinary JSON
// equality index. Specialized index variants retain the copying resolver path
// until they can provide the same snapshot guarantee.
func (ht *HatTrie) BorrowSQLIndexedSource(name, key, field string, value interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	if _, lower := hatSql.LowerIndexFieldName(field); lower {
		return nil, false, nil
	}
	ht.sqlIndexMu.Lock()
	index := ht.sqlJSONIndexes[key][field]
	ht.sqlIndexMu.Unlock()
	if index == nil {
		return nil, false, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index = ht.sqlJSONIndexes[key][field]
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
	if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
		return nil, false, err
	}
	valueKey, ok := sqlIndexValueKey(value)
	if !ok {
		return []SQLRow{}, true, nil
	}
	return index.rows[valueKey], true, nil
}
