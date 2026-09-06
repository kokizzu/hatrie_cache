package hatSql

import "errors"

var ErrSQLReplacingMergeKeyRequired = errors.New("hatSql: replacing merge key callback is required")

// SQLReplacingMergeKeyFunc returns the logical key for one row. It must be
// deterministic for the input batch.
type SQLReplacingMergeKeyFunc func(SQLRow) string

// SQLReplacingMergeVersionFunc returns a row version. Higher versions replace
// lower versions; equal versions use the later row in input order.
type SQLReplacingMergeVersionFunc func(SQLRow) (uint64, error)

// ReplaceSQLRows implements a deterministic ReplacingMergeTree-style merge.
// The output keeps the first-seen order of logical keys, while each key holds
// its newest version. When version is nil, the last row for each key wins.
// Returned maps are shallow copies, so changing an output row does not mutate
// the input batch or a replaced source row.
func ReplaceSQLRows(rows []SQLRow, key SQLReplacingMergeKeyFunc, version SQLReplacingMergeVersionFunc) ([]SQLRow, error) {
	if key == nil {
		return nil, ErrSQLReplacingMergeKeyRequired
	}
	if len(rows) == 0 {
		return nil, nil
	}

	positions := make(map[string]int, len(rows))
	selected := make([]SQLRow, 0, len(rows))
	versions := make([]uint64, 0, len(rows))
	for _, row := range rows {
		rowKey := key(row)
		rowVersion := uint64(0)
		if version != nil {
			var err error
			rowVersion, err = version(row)
			if err != nil {
				return nil, err
			}
		}

		position, exists := positions[rowKey]
		if !exists {
			positions[rowKey] = len(selected)
			selected = append(selected, row)
			versions = append(versions, rowVersion)
			continue
		}
		if version == nil || rowVersion >= versions[position] {
			selected[position] = row
			versions[position] = rowVersion
		}
	}

	merged := make([]SQLRow, len(selected))
	for index, row := range selected {
		merged[index] = cloneSQLReplacingMergeRow(row)
	}
	return merged, nil
}

func cloneSQLReplacingMergeRow(row SQLRow) SQLRow {
	if row == nil {
		return nil
	}
	clone := make(SQLRow, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}
