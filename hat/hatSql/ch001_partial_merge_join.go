package hatSql

import (
	"fmt"
	"sort"
)

// SQLJoinAlgorithm selects the physical algorithm for eligible equality joins.
// The empty value preserves the established planner. Partial merge is
// deliberately opt-in because it requires an ordered source resolver.
type SQLJoinAlgorithm string

const (
	SQLJoinAlgorithmDefault      SQLJoinAlgorithm = ""
	SQLJoinAlgorithmHash         SQLJoinAlgorithm = "hash"
	SQLJoinAlgorithmPartialMerge SQLJoinAlgorithm = "partial_merge"
)

func executeSQLPartialMergeJoin(rows []sqlExecRow, rightRows []SQLRow, leftAliases []string, leftQualifier, leftField, rightAlias, rightField string, control *sqlExecutionControl, maxRows int) ([]sqlExecRow, error) {
	left := rows
	if !sqlPartialMergeRowsOrdered(left, leftQualifier, leftField) {
		left = append([]sqlExecRow(nil), rows...)
		sort.SliceStable(left, func(first, second int) bool {
			comparison := sqlCompare(sqlField(left[first], leftQualifier, leftField), sqlField(left[second], leftQualifier, leftField))
			if comparison != 0 {
				return comparison < 0
			}
			return left[first].ordinals[leftQualifier] < left[second].ordinals[leftQualifier]
		})
	}
	right := wrapSQLSource(sqlSource{alias: rightAlias}, rightRows)
	nextCapacity := len(rows)
	if maxRows < nextCapacity {
		nextCapacity = maxRows
	}
	next := make([]sqlExecRow, 0, nextCapacity)
	leftIndex, rightIndex := 0, 0
	for leftIndex < len(left) && rightIndex < len(right) {
		if err := control.addJoinWork(1); err != nil {
			return nil, err
		}
		leftKey := sqlField(left[leftIndex], leftQualifier, leftField)
		rightKey := sqlField(right[rightIndex], rightAlias, rightField)
		if leftKey == nil {
			leftIndex++
			continue
		}
		if rightKey == nil {
			rightIndex++
			continue
		}
		comparison := sqlCompare(leftKey, rightKey)
		if comparison < 0 {
			leftIndex++
			continue
		}
		if comparison > 0 {
			rightIndex++
			continue
		}
		if !sqlPartialMergeJoinKeysEqual(leftKey, rightKey) {
			leftIndex++
			continue
		}
		leftEnd := leftIndex + 1
		for leftEnd < len(left) && sqlCompare(sqlField(left[leftEnd], leftQualifier, leftField), leftKey) == 0 && sqlPartialMergeJoinKeysEqual(sqlField(left[leftEnd], leftQualifier, leftField), leftKey) {
			leftEnd++
		}
		rightEnd := rightIndex + 1
		for rightEnd < len(right) && sqlCompare(sqlField(right[rightEnd], rightAlias, rightField), rightKey) == 0 && sqlPartialMergeJoinKeysEqual(sqlField(right[rightEnd], rightAlias, rightField), rightKey) {
			rightEnd++
		}
		for leftMatch := leftIndex; leftMatch < leftEnd; leftMatch++ {
			for rightMatch := rightIndex; rightMatch < rightEnd; rightMatch++ {
				if err := control.addJoinWork(1); err != nil {
					return nil, err
				}
				next = append(next, mergeSQLRows(left[leftMatch], right[rightMatch]))
				if len(next) > maxRows {
					return nil, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
				}
			}
		}
		leftIndex, rightIndex = leftEnd, rightEnd
	}

	aliases := append(append([]string(nil), leftAliases...), rightAlias)
	sort.SliceStable(next, func(first, second int) bool {
		for _, alias := range aliases {
			leftOrdinal, leftOK := next[first].ordinals[alias]
			rightOrdinal, rightOK := next[second].ordinals[alias]
			if !leftOK || !rightOK || leftOrdinal == rightOrdinal {
				continue
			}
			return leftOrdinal < rightOrdinal
		}
		return false
	})
	return next, nil
}

func sqlPartialMergeRowsOrdered(rows []sqlExecRow, qualifier, field string) bool {
	for index := 1; index < len(rows); index++ {
		if sqlCompare(sqlField(rows[index-1], qualifier, field), sqlField(rows[index], qualifier, field)) > 0 {
			return false
		}
	}
	return true
}

func sqlPartialMergeJoinKeysEqual(left, right interface{}) bool {
	return sqlBinaryValueWithCollation("=", left, right, SQLCollationBinary) == true
}
