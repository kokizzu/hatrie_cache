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
	return fillSQLRowsBounded(rows, spec, 0)
}

func fillSQLRowsBounded(rows []SQLRow, spec SQLWithFillSpec, maxRows int) ([]SQLRow, error) {
	if strings.TrimSpace(spec.Column) == "" {
		return nil, fmt.Errorf("%w: column is empty", ErrSQLWithFillInvalid)
	}
	if spec.From.IsZero() || spec.To.IsZero() || !spec.To.After(spec.From) {
		return nil, fmt.Errorf("%w: bounds must be non-zero and To must be after From", ErrSQLWithFillInvalid)
	}
	if spec.Step <= 0 {
		return nil, fmt.Errorf("%w: step must be positive", ErrSQLWithFillInvalid)
	}
	bucketCount, err := sqlFillBucketCount(spec)
	if err != nil {
		return nil, err
	}
	capacity := len(rows)
	if maxRows > 0 {
		if capacity < bucketCount {
			capacity = bucketCount
		}
		if capacity > maxRows {
			capacity = maxRows
		}
	}

	result := make([]SQLRow, 0, capacity)
	appendRow := func(row SQLRow) error {
		if maxRows > 0 && len(result) >= maxRows {
			return fmt.Errorf("%w: expanded result exceeds %d rows", ErrSQLWithFillInvalid, maxRows)
		}
		result = append(result, row)
		return nil
	}
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
			if err := appendRow(newSQLFillRow(spec.Template, spec.Column, cursor)); err != nil {
				return nil, err
			}
			var err error
			cursor, err = advanceSQLFillTime(cursor, spec.Step)
			if err != nil {
				return nil, err
			}
		}
		if err := appendRow(cloneSQLFillRow(row)); err != nil {
			return nil, err
		}
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
		if err := appendRow(newSQLFillRow(spec.Template, spec.Column, cursor)); err != nil {
			return nil, err
		}
		var err error
		cursor, err = advanceSQLFillTime(cursor, spec.Step)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func sqlFillBucketCount(spec SQLWithFillSpec) (int, error) {
	distance := spec.To.Sub(spec.From)
	if distance <= 0 {
		return 0, fmt.Errorf("%w: fill range overflows duration arithmetic", ErrSQLWithFillInvalid)
	}
	buckets := distance / spec.Step
	if distance%spec.Step != 0 {
		buckets++
	}
	maxInt := int(^uint(0) >> 1)
	if uint64(buckets) > uint64(maxInt) {
		return 0, fmt.Errorf("%w: fill range has too many buckets", ErrSQLWithFillInvalid)
	}
	return int(buckets), nil
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
