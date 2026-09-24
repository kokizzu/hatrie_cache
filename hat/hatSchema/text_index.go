package hatSchema

import (
	"fmt"
	"sort"
	"strings"

	"hatrie_cache/hat/hatSql"
)

// TextIndexBuildReport describes an atomically published positional text index.
// The index is opt-in and is maintained after publication by inserts and
// upserts.
type TextIndexBuildReport struct {
	Field    string
	Rows     int
	Tokens   int
	Postings int
	Attempts int
}

type materializedTextPosting struct {
	row       int
	positions []int
}

type materializedTextIndex struct {
	postings     map[string][]materializedTextPosting
	postingCount int
}

// BuildTextIndex builds and atomically installs a positional text index for a
// string column. Repeated tokens retain their positions, allowing exact phrase
// and bounded-proximity candidate lookup. Non-string and NULL values are
// ignored; the SQL executor still rechecks every candidate's predicate.
func (source *MaterializedSource) BuildTextIndex(field string) (TextIndexBuildReport, error) {
	if source == nil {
		return TextIndexBuildReport{}, ErrMaterializedSourceNil
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return TextIndexBuildReport{}, ErrMaterializedSourceColumnRequired
	}
	report := TextIndexBuildReport{Field: field}
	for {
		source.mu.RLock()
		if !source.hasColumnLocked(field) {
			source.mu.RUnlock()
			return TextIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, field)
		}
		generation := source.generation
		rows := append([]Row(nil), source.rows...)
		source.mu.RUnlock()

		index := &materializedTextIndex{postings: make(map[string][]materializedTextPosting)}
		for position, row := range rows {
			index.add(position, row[field])
		}
		report.Attempts++

		source.mu.Lock()
		if source.generation != generation {
			source.mu.Unlock()
			continue
		}
		if source.textIndexes == nil {
			source.textIndexes = make(map[string]*materializedTextIndex)
		}
		source.textIndexes[field] = index
		source.mu.Unlock()
		report.Rows = len(rows)
		report.Tokens = len(index.postings)
		report.Postings = index.postingCount
		return report, nil
	}
}

// HasTextIndex reports whether field has a maintained positional text index.
func (source *MaterializedSource) HasTextIndex(field string) bool {
	if source == nil {
		return false
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return false
	}
	source.mu.RLock()
	_, indexed := source.textIndexes[field]
	source.mu.RUnlock()
	return indexed
}

// LookupText returns rows whose indexed text contains query tokens in order,
// with at most maxGap intervening tokens between adjacent query tokens. It
// returns nil when field is not indexed or no rows match.
func (source *MaterializedSource) LookupText(field, query string, maxGap int) []Row {
	rows, _ := source.lookupText(field, query, maxGap)
	return rows
}

func (source *MaterializedSource) lookupText(field, query string, maxGap int) ([]Row, bool) {
	if source == nil || maxGap < 0 {
		return nil, false
	}
	field = strings.TrimSpace(field)
	source.mu.RLock()
	index := source.textIndexes[field]
	if index == nil {
		source.mu.RUnlock()
		return nil, false
	}
	positions := index.matchingRows(query, maxGap)
	rows := make([]Row, 0, len(positions))
	for _, position := range positions {
		rows = append(rows, cloneRow(source.rows[position]))
	}
	source.mu.RUnlock()
	return rows, true
}

func (source *MaterializedSource) addTextIndexesForInsertLocked(row Row, position int) {
	for field, index := range source.textIndexes {
		if index != nil {
			index.add(position, row[field])
		}
	}
}

func (source *MaterializedSource) replaceTextIndexesLocked(before, after Row, position int) {
	for field, index := range source.textIndexes {
		if index == nil {
			continue
		}
		oldValue, oldString := before[field].(string)
		newValue, newString := after[field].(string)
		if oldString && newString && oldValue == newValue {
			continue
		}
		index.remove(position, before[field])
		index.add(position, after[field])
	}
}

func (index *materializedTextIndex) add(row int, value interface{}) {
	grouped := materializedTextPositions(value)
	for token, positions := range grouped {
		postings := index.postings[token]
		at := sort.Search(len(postings), func(offset int) bool { return postings[offset].row >= row })
		if at < len(postings) && postings[at].row == row {
			postings[at].positions = append(postings[at].positions[:0], positions...)
			index.postings[token] = postings
			continue
		}
		postings = append(postings, materializedTextPosting{})
		copy(postings[at+1:], postings[at:])
		postings[at] = materializedTextPosting{row: row, positions: append([]int(nil), positions...)}
		index.postings[token] = postings
		index.postingCount++
	}
}

func (index *materializedTextIndex) remove(row int, value interface{}) {
	for token := range materializedTextPositions(value) {
		postings := index.postings[token]
		at := sort.Search(len(postings), func(offset int) bool { return postings[offset].row >= row })
		if at >= len(postings) || postings[at].row != row {
			continue
		}
		postings = append(postings[:at], postings[at+1:]...)
		if len(postings) == 0 {
			delete(index.postings, token)
		} else {
			index.postings[token] = postings
		}
		index.postingCount--
	}
}

func (index *materializedTextIndex) matchingRows(query string, maxGap int) []int {
	queryPositions := hatSql.TextTokenPositions(query)
	if len(queryPositions) == 0 {
		return nil
	}
	var candidates []materializedTextPosting
	for _, queryPosition := range queryPositions {
		postings, exists := index.postings[queryPosition.Token]
		if !exists {
			return nil
		}
		if candidates == nil || len(postings) < len(candidates) {
			candidates = postings
		}
	}

	positionsByQueryToken := make([][]int, len(queryPositions))
	rows := make([]int, 0, len(candidates))
	for _, candidate := range candidates {
		matched := true
		for queryIndex, queryPosition := range queryPositions {
			posting, exists := index.postingForRow(queryPosition.Token, candidate.row)
			if !exists {
				matched = false
				break
			}
			positionsByQueryToken[queryIndex] = posting.positions
		}
		if matched && materializedTextPositionsMatch(positionsByQueryToken, maxGap) {
			rows = append(rows, candidate.row)
		}
	}
	return rows
}

func (index *materializedTextIndex) postingForRow(token string, row int) (materializedTextPosting, bool) {
	postings := index.postings[token]
	at := sort.Search(len(postings), func(offset int) bool { return postings[offset].row >= row })
	if at >= len(postings) || postings[at].row != row {
		return materializedTextPosting{}, false
	}
	return postings[at], true
}

func materializedTextPositions(value interface{}) map[string][]int {
	text, ok := value.(string)
	if !ok {
		return nil
	}
	positions := hatSql.TextTokenPositions(text)
	if len(positions) == 0 {
		return nil
	}
	grouped := make(map[string][]int, len(positions))
	for _, position := range positions {
		grouped[position.Token] = append(grouped[position.Token], position.Position)
	}
	return grouped
}

func materializedTextPositionsMatch(positions [][]int, maxGap int) bool {
	if len(positions) == 0 {
		return false
	}
	for _, start := range positions[0] {
		last := start
		matched := true
		for queryIndex := 1; queryIndex < len(positions); queryIndex++ {
			nextPositions := positions[queryIndex]
			next := sort.Search(len(nextPositions), func(offset int) bool { return nextPositions[offset] > last })
			if next >= len(nextPositions) || nextPositions[next]-last-1 > maxGap {
				matched = false
				break
			}
			last = nextPositions[next]
		}
		if matched {
			return true
		}
	}
	return false
}
