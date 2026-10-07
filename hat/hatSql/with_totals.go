package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

// ErrSQLWithTotalsInvalid reports an unsupported or malformed WITH TOTALS
// clause.
var ErrSQLWithTotalsInvalid = errors.New("hatSql: invalid WITH TOTALS specification")

const sqlWithTotalsMarkerBase = "__hatrie_with_totals_grouping"

func sqlPrepareWithTotals(query *sqlQuery) error {
	if query == nil || !query.withTotals {
		return nil
	}
	if len(query.groupBy) == 0 {
		return fmt.Errorf("%w: GROUP BY is required", ErrSQLWithTotalsInvalid)
	}
	if len(query.groupingSets) != 0 {
		return fmt.Errorf("%w: GROUP BY ROLLUP, CUBE, or GROUPING SETS cannot be combined with WITH TOTALS", ErrSQLWithTotalsInvalid)
	}
	if len(query.unions) != 0 {
		return fmt.Errorf("%w: set operations cannot be combined with WITH TOTALS", ErrSQLWithTotalsInvalid)
	}
	if query.limit >= 0 || query.limitBy != nil {
		return fmt.Errorf("%w: LIMIT cannot be combined with WITH TOTALS", ErrSQLWithTotalsInvalid)
	}

	marker := sqlWithTotalsMarkerBase
	for {
		collision := false
		for _, selectItem := range query.selects {
			if strings.EqualFold(selectItem.alias, marker) {
				collision = true
				break
			}
		}
		if !collision {
			break
		}
		marker += "_1"
	}
	query.totalsMarker = marker
	query.selects = append(query.selects, sqlSelectItem{
		expr: sqlExpr{
			kind: "func",
			name: "GROUPING",
			args: []sqlExpr{cloneSQLExpr(query.groupBy[0])},
		},
		alias: marker,
	})
	query.groupingSets = [][]sqlExpr{cloneSQLExprs(query.groupBy), nil}
	query.groupingDimensions = cloneSQLExprs(query.groupBy)
	return nil
}

func sqlExtractWithTotals(result SQLQueryResult, marker string) (SQLQueryResult, error) {
	if marker == "" {
		return result, nil
	}
	markerIndex := -1
	columns := make([]string, 0, len(result.Columns)-1)
	for index, column := range result.Columns {
		if column == marker {
			markerIndex = index
			continue
		}
		columns = append(columns, column)
	}
	if markerIndex < 0 {
		return result, fmt.Errorf("%w: internal grouping marker is missing", ErrSQLWithTotalsInvalid)
	}

	rows := make([]SQLRow, 0, len(result.Rows))
	var totals SQLRow
	for _, row := range result.Rows {
		markerValue, ok := sqlNumber(row[marker])
		delete(row, marker)
		if ok && markerValue == 1 {
			if totals != nil {
				return result, fmt.Errorf("%w: multiple total rows were produced", ErrSQLWithTotalsInvalid)
			}
			totals = row
			continue
		}
		rows = append(rows, row)
	}
	result.Columns = columns
	result.Rows = rows
	result.Totals = totals
	return result, nil
}
