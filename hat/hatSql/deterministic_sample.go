package hatSql

import (
	"errors"
	"math"

	"github.com/cespare/xxhash/v2"
)

var (
	ErrSQLSamplingKeyRequired     = errors.New("hatSql: deterministic sampling key callback is required")
	ErrSQLSamplingInvalidFraction = errors.New("hatSql: deterministic sampling fraction must be finite and between zero and one")
)

// SQLSamplingKeyFunc returns the stable logical key used for sampling. Rows
// with the same key, fraction, and seed are selected together.
type SQLSamplingKeyFunc func(SQLRow) string

// SampleSQLRows selects a deterministic fraction of rows using a stable key
// hash and seed. Selection is independent of input order and partition
// boundaries; returned rows retain the order of the input batch.
func SampleSQLRows(rows []SQLRow, key SQLSamplingKeyFunc, fraction float64, seed uint64) ([]SQLRow, error) {
	if key == nil {
		return nil, ErrSQLSamplingKeyRequired
	}
	if math.IsNaN(fraction) || math.IsInf(fraction, 0) || fraction < 0 || fraction > 1 {
		return nil, ErrSQLSamplingInvalidFraction
	}
	if fraction == 0 || len(rows) == 0 {
		return nil, nil
	}
	if fraction == 1 {
		return append([]SQLRow(nil), rows...), nil
	}

	threshold := uint64(fraction * float64(^uint64(0)))
	selected := make([]SQLRow, 0, int(float64(len(rows))*fraction))
	for _, row := range rows {
		hash := mixSQLSamplingHash(xxhash.Sum64String(key(row)) ^ seed)
		if hash < threshold {
			selected = append(selected, row)
		}
	}
	if len(selected) == 0 {
		return nil, nil
	}
	return selected, nil
}

func mixSQLSamplingHash(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
