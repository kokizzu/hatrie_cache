package hatCache

import (
	"fmt"
	"sort"

	"hatrie_cache/hat/hatSql"
)

const maxSQLTextProximityGap = 1 << 20

type sqlJSONTextPosition struct {
	row      uint32
	position uint32
}

// ResolveSQLTextProximitySource resolves exact phrases and ordered proximity
// predicates from a lazily-built positional text sidecar. The SQL executor
// rechecks every candidate, so stale or conservative candidates cannot change
// query results.
func (ht *HatTrie) ResolveSQLTextProximitySource(name, key, field, query string, maxGap int) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	if maxGap < 0 || maxGap > maxSQLTextProximityGap {
		return nil, false, fmt.Errorf("CONTAINS_PROXIMITY distance must be a non-negative integer no larger than %d", maxSQLTextProximityGap)
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
	queryPositions := hatSql.TextTokenPositions(query)
	if len(queryPositions) == 0 {
		return []SQLRow{}, true, nil
	}
	ensureSQLJSONTextPositions(index, field)
	return sqlJSONTextProximityRows(index, queryPositions, maxGap), true, nil
}

// ResolveSQLTextProximityUnionSource resolves an OR of phrases against one
// positional sidecar. Matching row indexes are marked once and emitted from
// the shared source snapshot, which preserves source order without comparing
// or serializing arbitrary SQL rows.
func (ht *HatTrie) ResolveSQLTextProximityUnionSource(name, key, field string, queries []hatSql.SQLTextProximityQuery) ([]SQLRow, bool, error) {
	return ht.resolveSQLTextProximityUnionSource(name, key, field, queries)
}

// ResolveSQLTextProximityMultiFieldIntersectionSource resolves an AND of
// phrases or proximity predicates against positional sidecars. Each sidecar
// contributes source-order row IDs, which are intersected before rows are
// materialized.
func (ht *HatTrie) ResolveSQLTextProximityMultiFieldIntersectionSource(name, key string, queries []hatSql.SQLTextProximityFieldQuery) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	if len(queries) == 0 {
		return nil, false, nil
	}
	for _, query := range queries {
		if query.Query.MaxGap < 0 || query.Query.MaxGap > maxSQLTextProximityGap {
			return nil, false, fmt.Errorf("CONTAINS_PROXIMITY distance must be a non-negative integer no larger than %d", maxSQLTextProximityGap)
		}
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return nil, false, nil
		}
		return nil, false, err
	}
	fields := make([]string, 0, len(queries))
	indexes := make([]*sqlJSONTextIndex, 0, len(queries))
	for _, query := range queries {
		indexPosition := -1
		for index, field := range fields {
			if field == query.Field {
				indexPosition = index
				break
			}
		}
		if indexPosition < 0 {
			index := ht.sqlJSONTextIndexes[key][query.Field]
			if index == nil {
				return nil, false, nil
			}
			if err := refreshSQLJSONTextIndexSourceRows(index, query.Field, source, snapshot.rows); err != nil {
				return nil, false, err
			}
			ensureSQLJSONTextPositions(index, query.Field)
			fields = append(fields, query.Field)
			indexes = append(indexes, index)
			indexPosition = len(indexes) - 1
		}
		if len(hatSql.TextTokenPositions(query.Query.Query)) == 0 {
			return []SQLRow{}, true, nil
		}
		if len(indexes[indexPosition].rows) != len(snapshot.rows) {
			return nil, false, nil
		}
	}
	var matchedRows []int
	for queryIndex, query := range queries {
		indexPosition := 0
		for index, field := range fields {
			if field == query.Field {
				indexPosition = index
				break
			}
		}
		index := indexes[indexPosition]
		queryPositions := hatSql.TextTokenPositions(query.Query.Query)
		if queryIndex == 0 {
			matchedRows = sqlJSONTextProximityMatchingRows(index, queryPositions, query.Query.MaxGap, nil)
			if len(matchedRows) == 0 {
				return []SQLRow{}, true, nil
			}
			continue
		}
		filtered := matchedRows[:0]
		for _, rowIndex := range matchedRows {
			if sqlJSONTextRowMatchesProximity(index.positions, uint32(rowIndex), queryPositions, query.Query.MaxGap) {
				filtered = append(filtered, rowIndex)
			}
		}
		matchedRows = filtered
		if len(matchedRows) == 0 {
			return []SQLRow{}, true, nil
		}
	}
	rows := make([]SQLRow, 0, len(matchedRows))
	for _, rowIndex := range matchedRows {
		rows = append(rows, indexes[0].rows[rowIndex])
	}
	return rows, true, nil
}

func (ht *HatTrie) resolveSQLTextProximityUnionSource(name, key, field string, queries []hatSql.SQLTextProximityQuery) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	for _, query := range queries {
		if query.MaxGap < 0 || query.MaxGap > maxSQLTextProximityGap {
			return nil, false, fmt.Errorf("CONTAINS_PROXIMITY distance must be a non-negative integer no larger than %d", maxSQLTextProximityGap)
		}
	}
	if len(queries) == 1 {
		return ht.ResolveSQLTextProximitySource(name, key, field, queries[0].Query, queries[0].MaxGap)
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
	ensureSQLJSONTextPositions(index, field)
	matched := make([]bool, len(index.rows))
	for _, query := range queries {
		queryPositions := hatSql.TextTokenPositions(query.Query)
		if len(queryPositions) == 0 {
			continue
		}
		anchorToken := queryPositions[0].Token
		for _, token := range queryPositions[1:] {
			if len(index.tokens[token.Token]) < len(index.tokens[anchorToken]) {
				anchorToken = token.Token
			}
		}
		for _, rowIndex := range index.tokens[anchorToken] {
			if rowIndex < 0 || rowIndex >= len(index.rows) || !sqlJSONTextRowMatchesProximity(index.positions, uint32(rowIndex), queryPositions, query.MaxGap) {
				continue
			}
			matched[rowIndex] = true
		}
	}
	rows := make([]SQLRow, 0)
	for rowIndex, rowMatched := range matched {
		if rowMatched {
			rows = append(rows, index.rows[rowIndex])
		}
	}
	return rows, true, nil
}

func sqlJSONTextProximityRows(index *sqlJSONTextIndex, queryPositions []hatSql.TextTokenPosition, maxGap int) []SQLRow {
	matchedRows := sqlJSONTextProximityMatchingRows(index, queryPositions, maxGap, nil)
	matched := make([]SQLRow, 0, len(matchedRows))
	for _, rowIndex := range matchedRows {
		matched = append(matched, index.rows[rowIndex])
	}
	return matched
}

func sqlJSONTextProximityMatchingRows(index *sqlJSONTextIndex, queryPositions []hatSql.TextTokenPosition, maxGap int, matched []int) []int {
	if len(queryPositions) == 0 {
		return matched[:0]
	}
	anchorToken := queryPositions[0].Token
	for _, token := range queryPositions[1:] {
		if len(index.tokens[token.Token]) < len(index.tokens[anchorToken]) {
			anchorToken = token.Token
		}
	}
	anchorRows := index.tokens[anchorToken]
	if len(anchorRows) == 0 {
		return matched[:0]
	}
	if cap(matched) < len(anchorRows) {
		matched = make([]int, 0, len(anchorRows))
	} else {
		matched = matched[:0]
	}
	for _, rowIndex := range anchorRows {
		if rowIndex < 0 || rowIndex >= len(index.rows) || !sqlJSONTextRowMatchesProximity(index.positions, uint32(rowIndex), queryPositions, maxGap) {
			continue
		}
		matched = append(matched, rowIndex)
	}
	return matched
}

func ensureSQLJSONTextPositions(index *sqlJSONTextIndex, field string) {
	if index.positionsReady {
		return
	}
	positions := make(map[string][]sqlJSONTextPosition)
	for rowIndex, row := range index.rows {
		text, ok := row[field].(string)
		if !ok {
			continue
		}
		for _, token := range hatSql.TextTokenPositions(text) {
			positions[token.Token] = append(positions[token.Token], sqlJSONTextPosition{row: uint32(rowIndex), position: uint32(token.Position)})
		}
	}
	index.positions = positions
	index.positionsReady = true
}

func sqlJSONTextRowMatchesProximity(positions map[string][]sqlJSONTextPosition, row uint32, query []hatSql.TextTokenPosition, maxGap int) bool {
	if len(query) == 0 {
		return false
	}
	first := positions[query[0].Token]
	start, end := sqlJSONTextPositionRange(first, row)
	for positionIndex := start; positionIndex < end; positionIndex++ {
		lastPosition := first[positionIndex].position
		matched := true
		for queryIndex := 1; queryIndex < len(query); queryIndex++ {
			posting := positions[query[queryIndex].Token]
			postingStart, postingEnd := sqlJSONTextPositionRange(posting, row)
			next := sort.Search(postingEnd-postingStart, func(index int) bool {
				return posting[postingStart+index].position > lastPosition
			})
			if next == postingEnd-postingStart {
				matched = false
				break
			}
			candidate := posting[postingStart+next].position
			if uint64(candidate)-uint64(lastPosition)-1 > uint64(maxGap) {
				matched = false
				break
			}
			lastPosition = candidate
		}
		if matched {
			return true
		}
	}
	return false
}

func sqlJSONTextPositionRange(posting []sqlJSONTextPosition, row uint32) (int, int) {
	start := sort.Search(len(posting), func(index int) bool { return posting[index].row >= row })
	end := start + sort.Search(len(posting)-start, func(index int) bool { return posting[start+index].row > row })
	return start, end
}
