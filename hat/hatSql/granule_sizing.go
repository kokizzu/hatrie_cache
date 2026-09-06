package hatSql

import (
	"errors"
	"math"
)

const (
	defaultGranuleMinRows           = 256
	defaultGranuleRows              = 8192
	defaultGranuleMaxRows           = 65536
	defaultGranuleTargetSelectivity = 0.10
)

var ErrGranuleSizingOptionsInvalid = errors.New("hatriecache: granule sizing options are invalid")

// GranuleSizingOptions controls how observed predicate selectivity changes a
// later scan granule size. Zero values use conservative defaults.
type GranuleSizingOptions struct {
	MinRows           int
	DefaultRows       int
	MaxRows           int
	TargetSelectivity float64
}

// GranuleSizingPolicy suggests a bounded granule size from observed rows. It
// is immutable after construction and has no retained observation state.
type GranuleSizingPolicy struct {
	minRows           int
	defaultRows       int
	maxRows           int
	targetSelectivity float64
}

// NewGranuleSizingPolicy validates options and fills zero fields with sane
// defaults for a columnar scan.
func NewGranuleSizingPolicy(options GranuleSizingOptions) (GranuleSizingPolicy, error) {
	if options.MinRows == 0 {
		options.MinRows = defaultGranuleMinRows
	}
	if options.DefaultRows == 0 {
		options.DefaultRows = defaultGranuleRows
	}
	if options.MaxRows == 0 {
		options.MaxRows = defaultGranuleMaxRows
	}
	if options.TargetSelectivity == 0 {
		options.TargetSelectivity = defaultGranuleTargetSelectivity
	}
	if options.MinRows < 1 || options.DefaultRows < options.MinRows || options.MaxRows < options.DefaultRows ||
		math.IsNaN(options.TargetSelectivity) || math.IsInf(options.TargetSelectivity, 0) ||
		options.TargetSelectivity <= 0 || options.TargetSelectivity > 1 {
		return GranuleSizingPolicy{}, ErrGranuleSizingOptionsInvalid
	}
	return GranuleSizingPolicy{
		minRows:           options.MinRows,
		defaultRows:       options.DefaultRows,
		maxRows:           options.MaxRows,
		targetSelectivity: options.TargetSelectivity,
	}, nil
}

// Suggest returns a bounded next granule size. Selective observations shrink
// granules, dense observations grow them, and incomplete observations retain
// the current bounded size.
func (policy GranuleSizingPolicy) Suggest(currentRows int, scannedRows, matchedRows uint64) int {
	if policy.minRows == 0 || policy.defaultRows == 0 || policy.maxRows == 0 {
		return 0
	}
	currentRows = clampGranuleRows(currentRows, policy.minRows, policy.defaultRows, policy.maxRows)
	if scannedRows == 0 || matchedRows > scannedRows {
		return currentRows
	}
	if matchedRows == 0 {
		return policy.minRows
	}
	selectivity := float64(matchedRows) / float64(scannedRows)
	suggested := int(math.Round(float64(currentRows) * selectivity / policy.targetSelectivity))
	return clampGranuleRows(suggested, policy.minRows, policy.defaultRows, policy.maxRows)
}

func clampGranuleRows(rows, minRows, defaultRows, maxRows int) int {
	if rows <= 0 {
		rows = defaultRows
	}
	if rows < minRows {
		return minRows
	}
	if rows > maxRows {
		return maxRows
	}
	return rows
}
