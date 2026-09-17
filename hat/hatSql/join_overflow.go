package hatSql

import "fmt"

// SQLJoinOverflowPolicy controls how an explicit MaxJoinBytes join budget is
// handled. The zero value keeps the established automatic behavior.
type SQLJoinOverflowPolicy string

const (
	// SQLJoinOverflowAuto preserves the legacy behavior. Eligible equality
	// joins use configured spill settings; other joins use their normal path.
	SQLJoinOverflowAuto SQLJoinOverflowPolicy = ""
	// SQLJoinOverflowReject keeps the join in memory and rejects a materialized
	// input that exceeds MaxJoinBytes instead of silently falling back.
	SQLJoinOverflowReject SQLJoinOverflowPolicy = "reject"
	// SQLJoinOverflowSpill requires the bounded spill hash-join path for an
	// eligible direct equality join and rejects unsupported shapes.
	SQLJoinOverflowSpill SQLJoinOverflowPolicy = "spill"
)

func (policy SQLJoinOverflowPolicy) valid() bool {
	switch policy {
	case SQLJoinOverflowAuto, SQLJoinOverflowReject, SQLJoinOverflowSpill:
		return true
	default:
		return false
	}
}

func sqlJoinMaterializedInputBudgetError(options SQLQueryOptions, rows []SQLRow) error {
	if options.JoinOverflowPolicy != SQLJoinOverflowReject || options.MaxJoinBytes <= 0 {
		return nil
	}
	actual := sqlRowsBytes(rows)
	if actual <= options.MaxJoinBytes {
		return nil
	}
	return fmt.Errorf("SQL join input exceeds the %d byte budget (%d bytes); use JoinOverflowPolicy %q with spill settings or increase MaxJoinBytes", options.MaxJoinBytes, actual, SQLJoinOverflowSpill)
}
