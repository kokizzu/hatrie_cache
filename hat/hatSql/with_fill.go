package hatSql

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrSQLWithFillInvalid = errors.New("hatSql: invalid WITH FILL specification")

// SQLWithFillSpec describes an ordered time-series gap-filling operation.
// From is inclusive and To is exclusive. Existing rows are expected to be in
// ascending order by Column; generated rows use Template as their base row.
type SQLWithFillSpec struct {
	Column   string
	From     time.Time
	To       time.Time
	Step     time.Duration
	Template Row
}

// FillSQLRows inserts template rows at missing time steps without mutating
// the input rows or template. Existing rows remain in their original order;
// only the [From, To) interval is filled.
func FillSQLRows(rows []SQLRow, spec SQLWithFillSpec) ([]SQLRow, error) {
	if strings.TrimSpace(spec.Column) == "" {
		return nil, fmt.Errorf("%w: column is empty", ErrSQLWithFillInvalid)
	}
	if spec.From.IsZero() || spec.To.IsZero() || !spec.To.After(spec.From) {
		return nil, fmt.Errorf("%w: bounds must be non-zero and To must be after From", ErrSQLWithFillInvalid)
	}
	if spec.Step <= 0 {
		return nil, fmt.Errorf("%w: step must be positive", ErrSQLWithFillInvalid)
	}

	result := make([]SQLRow, 0, len(rows))
	cursor := spec.From
	var previous time.Time
	for index, row := range rows {
		value, ok := row[spec.Column]
		if !ok {
			return nil, fmt.Errorf("%w: row %d is missing column %q", ErrSQLWithFillInvalid, index, spec.Column)
		}
		at, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("%w: row %d column %q is not time.Time", ErrSQLWithFillInvalid, index, spec.Column)
		}
		if index > 0 && at.Before(previous) {
			return nil, fmt.Errorf("%w: rows are not ordered at row %d", ErrSQLWithFillInvalid, index)
		}
		if at.Before(spec.From) || !at.Before(spec.To) {
			return nil, fmt.Errorf("%w: row %d is outside the half-open fill interval", ErrSQLWithFillInvalid, index)
		}
		for cursor.Before(at) {
			result = append(result, newSQLFillRow(spec.Template, spec.Column, cursor))
			var err error
			cursor, err = advanceSQLFillTime(cursor, spec.Step)
			if err != nil {
				return nil, err
			}
		}
		result = append(result, cloneSQLFillRow(row))
		if cursor.Equal(at) {
			var err error
			cursor, err = advanceSQLFillTime(at, spec.Step)
			if err != nil {
				return nil, err
			}
		}
		previous = at
	}
	for cursor.Before(spec.To) {
		result = append(result, newSQLFillRow(spec.Template, spec.Column, cursor))
		var err error
		cursor, err = advanceSQLFillTime(cursor, spec.Step)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func newSQLFillRow(template Row, column string, at time.Time) Row {
	row := cloneSQLFillRow(template)
	if row == nil {
		row = make(Row, 1)
	}
	row[column] = at
	return row
}

func cloneSQLFillRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}

func advanceSQLFillTime(value time.Time, step time.Duration) (time.Time, error) {
	next := value.Add(step)
	if !next.After(value) {
		return time.Time{}, fmt.Errorf("%w: time range overflows", ErrSQLWithFillInvalid)
	}
	return next, nil
}
