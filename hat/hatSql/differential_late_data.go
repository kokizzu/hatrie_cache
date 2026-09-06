package hatSql

import (
	"errors"
	"fmt"
)

var (
	ErrDifferentialLateDataPolicyInvalid = errors.New("hatSql: invalid differential late-data policy")
	ErrDifferentialLateDataRejected      = errors.New("hatSql: differential late data rejected")
)

// DifferentialLateDataPolicy controls updates whose Time is less than the
// caller's frontier. Accept retains them, Drop removes them, and Reject
// returns an error without returning partial output.
type DifferentialLateDataPolicy uint8

const (
	DifferentialLateDataAccept DifferentialLateDataPolicy = iota
	DifferentialLateDataDrop
	DifferentialLateDataReject
)

// ApplyDifferentialLateDataPolicy applies an explicit late-data policy to a
// batch. An update at exactly frontier is on time. The input order is
// preserved, and returned rows own cloned row maps. The input is not mutated.
// Each call is independent and does not advance the frontier.
func ApplyDifferentialLateDataPolicy(rows []DifferentialRow, frontier uint64, policy DifferentialLateDataPolicy) ([]DifferentialRow, error) {
	if policy > DifferentialLateDataReject {
		return nil, fmt.Errorf("policy %d: %w", policy, ErrDifferentialLateDataPolicyInvalid)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if policy == DifferentialLateDataReject {
		for _, row := range rows {
			if row.Time < frontier {
				return nil, fmt.Errorf("key %q at time %d is before frontier %d: %w", row.Key, row.Time, frontier, ErrDifferentialLateDataRejected)
			}
		}
		return cloneDifferentialLateDataRows(rows), nil
	}

	filtered := make([]DifferentialRow, 0, len(rows))
	for _, row := range rows {
		if policy == DifferentialLateDataDrop && row.Time < frontier {
			continue
		}
		row.Row = cloneDifferentialRow(row.Row)
		filtered = append(filtered, row)
	}
	if len(filtered) == 0 {
		return nil, nil
	}
	return filtered, nil
}

func cloneDifferentialLateDataRows(rows []DifferentialRow) []DifferentialRow {
	cloned := make([]DifferentialRow, len(rows))
	for index, row := range rows {
		row.Row = cloneDifferentialRow(row.Row)
		cloned[index] = row
	}
	return cloned
}
