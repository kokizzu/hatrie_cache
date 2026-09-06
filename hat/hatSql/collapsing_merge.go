package hatSql

import (
	"errors"
	"fmt"
)

var (
	ErrSQLCollapsingMergeKeyRequired = errors.New("hatSql: collapsing merge key callback is required")
	ErrSQLCollapsingMergeSignRequired = errors.New("hatSql: collapsing merge sign callback is required")
	ErrSQLCollapsingMergeInvalidSign = errors.New("hatSql: collapsing merge sign must be -1 or 1")
)

// SQLCollapsingMergeSignFunc returns the sign for one row. Only -1 and 1 are
// valid values.
type SQLCollapsingMergeSignFunc func(SQLRow) (int, error)

// CollapseSQLRows implements a deterministic CollapsingMergeTree-style merge.
// Rows with the same key and opposite signs cancel; the latest unmatched
// opposite row is selected. Surviving rows retain input order and their maps
// are shallow copies. Unmatched rows are preserved so the caller can observe
// incomplete cancellation explicitly.
func CollapseSQLRows(rows []SQLRow, key SQLReplacingMergeKeyFunc, sign SQLCollapsingMergeSignFunc) ([]SQLRow, error) {
	if key == nil {
		return nil, ErrSQLCollapsingMergeKeyRequired
	}
	if sign == nil {
		return nil, ErrSQLCollapsingMergeSignRequired
	}
	if len(rows) == 0 {
		return nil, nil
	}

	type entry struct {
		row    SQLRow
		active bool
	}
	entries := make([]entry, 0, len(rows))
	positive := make(map[string][]int, len(rows))
	negative := make(map[string][]int, len(rows))
	for _, row := range rows {
		rowKey := key(row)
		rowSign, err := sign(row)
		if err != nil {
			return nil, err
		}
		if rowSign != -1 && rowSign != 1 {
			return nil, fmt.Errorf("%w: %d", ErrSQLCollapsingMergeInvalidSign, rowSign)
		}

		var opposite map[string][]int
		var own map[string][]int
		if rowSign == 1 {
			opposite = negative
			own = positive
		} else {
			opposite = positive
			own = negative
		}
		candidates := opposite[rowKey]
		if len(candidates) > 0 {
			last := len(candidates) - 1
			entries[candidates[last]].active = false
			if last == 0 {
				delete(opposite, rowKey)
			} else {
				opposite[rowKey] = candidates[:last]
			}
			continue
		}

		position := len(entries)
		entries = append(entries, entry{row: row, active: true})
		own[rowKey] = append(own[rowKey], position)
	}

	merged := make([]SQLRow, 0, len(entries))
	for _, item := range entries {
		if item.active {
			merged = append(merged, cloneSQLReplacingMergeRow(item.row))
		}
	}
	if len(merged) == 0 {
		return nil, nil
	}
	return merged, nil
}
