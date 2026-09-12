package hatSql

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// SQLWithFillInterpolation selects how a generated row obtains a value for a
// selected output column. An omitted policy in SQL syntax means PREVIOUS.
type SQLWithFillInterpolation string

const (
	SQLWithFillInterpolationPrevious SQLWithFillInterpolation = "PREVIOUS"
	SQLWithFillInterpolationNext     SQLWithFillInterpolation = "NEXT"
	SQLWithFillInterpolationLinear   SQLWithFillInterpolation = "LINEAR"
)

func validateSQLWithFillInterpolations(interpolations map[string]SQLWithFillInterpolation) error {
	for column, policy := range interpolations {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("%w: interpolation column is empty", ErrSQLWithFillInvalid)
		}
		switch policy {
		case SQLWithFillInterpolationPrevious, SQLWithFillInterpolationNext, SQLWithFillInterpolationLinear:
		default:
			return fmt.Errorf("%w: unsupported interpolation policy %q for column %q", ErrSQLWithFillInvalid, policy, column)
		}
	}
	return nil
}

func resolveSQLWithFillInterpolations(interpolations map[string]SQLWithFillInterpolation, columns []string) (map[string]SQLWithFillInterpolation, error) {
	if len(interpolations) == 0 {
		return nil, nil
	}
	resolved := make(map[string]SQLWithFillInterpolation, len(interpolations))
	for column, policy := range interpolations {
		var actual string
		for _, candidate := range columns {
			if strings.EqualFold(column, candidate) {
				actual = candidate
				break
			}
		}
		if actual == "" {
			return nil, fmt.Errorf("%w: interpolation column %q must be selected", ErrSQLWithFillInvalid, column)
		}
		if _, exists := resolved[actual]; exists {
			return nil, fmt.Errorf("%w: duplicate interpolation column %q", ErrSQLWithFillInvalid, actual)
		}
		resolved[actual] = policy
	}
	return resolved, nil
}

func fillSQLRowsInterpolated(rows []SQLRow, spec SQLWithFillSpec, bucketCount, maxRows int) ([]SQLRow, error) {
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
	var previousAt time.Time
	var previous Row
	for index, row := range rows {
		value, ok := row[spec.Column]
		if !ok {
			return nil, fmt.Errorf("%w: row %d is missing column %q", ErrSQLWithFillInvalid, index, spec.Column)
		}
		at, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("%w: row %d column %q is not time.Time", ErrSQLWithFillInvalid, index, spec.Column)
		}
		if index > 0 && at.Before(previousAt) {
			return nil, fmt.Errorf("%w: rows are not ordered at row %d", ErrSQLWithFillInvalid, index)
		}
		if at.Before(spec.From) || !at.Before(spec.To) {
			return nil, fmt.Errorf("%w: row %d is outside the half-open fill interval", ErrSQLWithFillInvalid, index)
		}
		for cursor.Before(at) {
			filled := newSQLFillRow(spec.Template, spec.Column, cursor)
			applySQLWithFillInterpolations(filled, spec.Interpolation, previous, previousAt, row, at, cursor)
			if err := appendRow(filled); err != nil {
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
		previous, previousAt = row, at
	}
	for cursor.Before(spec.To) {
		filled := newSQLFillRow(spec.Template, spec.Column, cursor)
		applySQLWithFillInterpolations(filled, spec.Interpolation, previous, previousAt, nil, time.Time{}, cursor)
		if err := appendRow(filled); err != nil {
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

func applySQLWithFillInterpolations(row Row, interpolations map[string]SQLWithFillInterpolation, previous Row, previousAt time.Time, next Row, nextAt, at time.Time) {
	for column, policy := range interpolations {
		if _, exists := row[column]; !exists {
			row[column] = nil
		}
		switch policy {
		case SQLWithFillInterpolationPrevious:
			if previous != nil {
				row[column] = previous[column]
			}
		case SQLWithFillInterpolationNext:
			if next != nil {
				row[column] = next[column]
			}
		case SQLWithFillInterpolationLinear:
			if previous == nil || next == nil || previousAt.IsZero() || nextAt.IsZero() {
				continue
			}
			left, leftOK := sqlNumber(previous[column])
			right, rightOK := sqlNumber(next[column])
			span := nextAt.Sub(previousAt)
			offset := at.Sub(previousAt)
			if !leftOK || !rightOK || span <= 0 || offset < 0 || offset > span {
				continue
			}
			value := left + (right-left)*(float64(offset)/float64(span))
			if isFiniteSQLNumber(value) {
				row[column] = value
			}
		}
	}
}

func sortedSQLWithFillInterpolationColumns(interpolations map[string]SQLWithFillInterpolation) []string {
	columns := make([]string, 0, len(interpolations))
	for column := range interpolations {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}
