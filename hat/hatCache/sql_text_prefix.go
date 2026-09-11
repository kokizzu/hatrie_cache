package hatCache

import (
	"sort"
	"strings"

	"hatrie_cache/hat/hatSql"
)

func sortedSQLTextTokenKeys(tokens map[string][]int) []string {
	keys := make([]string, 0, len(tokens))
	for token := range tokens {
		keys = append(keys, token)
	}
	sort.Strings(keys)
	return keys
}

// ResolveSQLTextPrefixSource resolves candidates whose indexed field contains
// a token beginning with one normalized prefix token. The SQL executor repeats
// the predicate against each candidate before publishing results.
func (ht *HatTrie) ResolveSQLTextPrefixSource(name, key, field, prefix string) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONTextIndexes[key][field]
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
	if err := refreshSQLJSONTextIndexSourceRows(index, field, source, snapshot.rows); err != nil {
		return nil, false, err
	}
	prefixTokens := hatSql.TextTokens(prefix)
	if len(prefixTokens) != 1 || len(index.tokenKeys) == 0 {
		return []SQLRow{}, true, nil
	}
	prefixToken := prefixTokens[0]
	start := sort.SearchStrings(index.tokenKeys, prefixToken)
	indices := make([]int, 0)
	for tokenIndex := start; tokenIndex < len(index.tokenKeys); tokenIndex++ {
		token := index.tokenKeys[tokenIndex]
		if !strings.HasPrefix(token, prefixToken) {
			break
		}
		indices = append(indices, index.tokens[token]...)
	}
	if len(indices) == 0 {
		return []SQLRow{}, true, nil
	}
	sort.Ints(indices)
	rows := make([]SQLRow, 0, len(indices))
	last := -1
	for _, rowIndex := range indices {
		if rowIndex == last || rowIndex < 0 || rowIndex >= len(index.rows) {
			continue
		}
		last = rowIndex
		rows = append(rows, index.rows[rowIndex])
	}
	return rows, true, nil
}
