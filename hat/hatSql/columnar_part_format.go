package hatSql

import "fmt"

// SQLColumnarPartFormat identifies the physical organization of a columnar
// part. Compact parts keep columns together; wide parts allow independent
// column files or streams.
type SQLColumnarPartFormat string

const (
	SQLColumnarPartCompact SQLColumnarPartFormat = "compact"
	SQLColumnarPartWide    SQLColumnarPartFormat = "wide"
)

// SQLColumnarPartFormatPolicy controls the compact-part admission boundary.
// A part is compact only when both its row count and estimated encoded byte
// width are at or below their respective limits.
type SQLColumnarPartFormatPolicy struct {
	MaxCompactRows  int64
	MaxCompactBytes int64
}

// DefaultSQLColumnarPartFormatPolicy returns the conservative default for
// small parts. Storage adapters can replace either limit for their workload.
func DefaultSQLColumnarPartFormatPolicy() SQLColumnarPartFormatPolicy {
	return SQLColumnarPartFormatPolicy{
		MaxCompactRows:  10_000,
		MaxCompactBytes: 1 << 20,
	}
}

// SelectSQLColumnarPartFormat chooses compact or wide organization from the
// part row count and estimated encoded byte width. It performs no allocation
// and does not inspect or modify the part data.
func SelectSQLColumnarPartFormat(rows, estimatedBytes int64, policy SQLColumnarPartFormatPolicy) (SQLColumnarPartFormat, error) {
	if rows < 0 {
		return "", fmt.Errorf("columnar part row count must be non-negative: %d", rows)
	}
	if estimatedBytes < 0 {
		return "", fmt.Errorf("columnar part byte width must be non-negative: %d", estimatedBytes)
	}
	if policy.MaxCompactRows <= 0 {
		return "", fmt.Errorf("columnar compact row limit must be positive: %d", policy.MaxCompactRows)
	}
	if policy.MaxCompactBytes <= 0 {
		return "", fmt.Errorf("columnar compact byte limit must be positive: %d", policy.MaxCompactBytes)
	}
	if rows <= policy.MaxCompactRows && estimatedBytes <= policy.MaxCompactBytes {
		return SQLColumnarPartCompact, nil
	}
	return SQLColumnarPartWide, nil
}
