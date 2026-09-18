package hatSql

import (
	"time"
)

// AsOfJoinRow is a left-side row evaluated at At against a TemporalTable.
type AsOfJoinRow struct {
	Key string
	At  time.Time
	Row Row
}

// AsOfJoinMatch is an inner ASOF join result. Right is the latest version for
// Key whose timestamp is not after LeftAt.
type AsOfJoinMatch struct {
	Key     string
	LeftAt  time.Time
	RightAt time.Time
	Left    Row
	Right   Row
}

// AsOfJoin returns the latest right-side version at or before each left-side
// row. Results retain left-side input order, and matched rows are cloned so
// callers cannot mutate the temporal table through the result.
func (table *TemporalTable) AsOfJoin(rows []AsOfJoinRow) []AsOfJoinMatch {
	if table == nil || len(rows) == 0 {
		return nil
	}

	type cursor struct {
		at    time.Time
		index int
	}
	matches := make([]AsOfJoinMatch, 0, len(rows))
	var currentKey string
	var current cursor
	var cursors map[string]cursor
	haveCurrent := false
	for _, left := range rows {
		versions := table.versions[left.Key]
		if len(versions) == 0 {
			continue
		}

		index := -1
		previous, hasPrevious := cursor{}, false
		if haveCurrent && left.Key == currentKey {
			previous, hasPrevious = current, true
		} else if cursors != nil {
			previous, hasPrevious = cursors[left.Key]
		}
		if hasPrevious && !left.At.Before(previous.at) {
			index = previous.index
			for index+1 < len(versions) && !versions[index+1].At.After(left.At) {
				index++
			}
		} else {
			index = temporalAsOfIndex(versions, left.At)
		}
		if index < 0 {
			continue
		}
		next := cursor{at: left.At, index: index}
		if haveCurrent && left.Key == currentKey {
			current = next
		} else {
			if haveCurrent {
				if cursors == nil {
					cursors = make(map[string]cursor)
				}
				cursors[currentKey] = current
			}
			if cursors != nil {
				cursors[left.Key] = next
			}
			currentKey, current, haveCurrent = left.Key, next, true
		}
		version := versions[index]
		matches = append(matches, AsOfJoinMatch{
			Key:     left.Key,
			LeftAt:  left.At,
			RightAt: version.At,
			Left:    CloneRows([]Row{left.Row})[0],
			Right:   CloneRows([]Row{version.Row})[0],
		})
	}
	return matches
}
