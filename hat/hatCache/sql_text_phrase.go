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
	anchorToken := queryPositions[0].Token
	for _, token := range queryPositions[1:] {
		if len(index.tokens[token.Token]) < len(index.tokens[anchorToken]) {
			anchorToken = token.Token
		}
	}
	anchorRows := index.tokens[anchorToken]
	if len(anchorRows) == 0 {
		return []SQLRow{}, true, nil
	}
	matched := make([]SQLRow, 0, len(anchorRows))
	for _, rowIndex := range anchorRows {
		if rowIndex < 0 || rowIndex >= len(index.rows) || !sqlJSONTextRowMatchesProximity(index.positions, uint32(rowIndex), queryPositions, maxGap) {
			continue
		}
		matched = append(matched, index.rows[rowIndex])
	}
	return matched, true, nil
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
