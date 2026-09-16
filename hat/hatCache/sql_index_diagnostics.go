package hatCache

import "hatrie_cache/hat/hatSql"

// ResolveSQLIndexDiagnostics reports the optional JSON path skip-index work
// used by EXPLAIN ANALYZE. It is deliberately separate from candidate lookup
// so existing indexed queries keep their established result and allocation
// behavior.
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
	index := ht.sqlJSONPathSkipIndexes[key][field]
	if index == nil {
		return hatSql.SQLIndexDiagnostics{}, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return hatSql.SQLIndexDiagnostics{}, false, nil
		}
		return hatSql.SQLIndexDiagnostics{}, false, err
	}
	if err := refreshSQLJSONPathSkipIndexSource(index, source, snapshot.rows); err != nil {
		return hatSql.SQLIndexDiagnostics{}, false, err
	}
	diagnostics, available := sqlJSONPathSkipDiagnostics(index, field, value)
	return diagnostics, available, nil
}
