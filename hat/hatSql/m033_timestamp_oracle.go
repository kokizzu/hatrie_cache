package hatSql

import (
	"errors"
	"sync/atomic"
)

var (
	ErrSQLLogicalTimestampOracleNil          = errors.New("hatSql: logical timestamp oracle is nil")
	ErrSQLLogicalTimestampOracleCountInvalid = errors.New("hatSql: logical timestamp reservation count must be positive")
	ErrSQLLogicalTimestampOracleExhausted    = errors.New("hatSql: logical timestamp oracle is exhausted")
)

// SQLLogicalTimestampRange is one contiguous, exclusively owned timestamp
// range returned by SQLLogicalTimestampOracle.Reserve.
type SQLLogicalTimestampRange struct {
	Start uint64
	End   uint64
}

// Count returns the number of timestamps in the range.
func (timestampRange SQLLogicalTimestampRange) Count() uint64 {
	if timestampRange.Start == 0 || timestampRange.End < timestampRange.Start {
		return 0
	}
	return timestampRange.End - timestampRange.Start + 1
}

// SQLLogicalTimestampOracle provides monotone process-local logical
// timestamps. It is opt-in and does not claim cross-process consensus.
type SQLLogicalTimestampOracle struct {
	next atomic.Uint64
}

// NewSQLLogicalTimestampOracle creates an oracle whose next reservation starts
// after initial. Initial is useful when restoring a persisted frontier.
func NewSQLLogicalTimestampOracle(initial uint64) *SQLLogicalTimestampOracle {
	oracle := &SQLLogicalTimestampOracle{}
	oracle.next.Store(initial)
	return oracle
}

// Current returns the latest observed or reserved timestamp. A nil oracle
// returns zero.
func (oracle *SQLLogicalTimestampOracle) Current() uint64 {
	if oracle == nil {
		return 0
	}
	return oracle.next.Load()
}

// Next reserves one timestamp without allocating.
func (oracle *SQLLogicalTimestampOracle) Next() (uint64, error) {
	timestampRange, err := oracle.Reserve(1)
	if err != nil {
		return 0, err
	}
	return timestampRange.Start, nil
}

// Reserve atomically reserves count contiguous timestamps. Reserving a batch
// uses one atomic update instead of one update per source record.
func (oracle *SQLLogicalTimestampOracle) Reserve(count int) (SQLLogicalTimestampRange, error) {
	if oracle == nil {
		return SQLLogicalTimestampRange{}, ErrSQLLogicalTimestampOracleNil
	}
	if count <= 0 {
		return SQLLogicalTimestampRange{}, ErrSQLLogicalTimestampOracleCountInvalid
	}
	increment := uint64(count)
	maximum := ^uint64(0)
	current := oracle.next.Load()
	for {
		if current > maximum-increment {
			return SQLLogicalTimestampRange{}, ErrSQLLogicalTimestampOracleExhausted
		}
		next := current + increment
		if oracle.next.CompareAndSwap(current, next) {
			return SQLLogicalTimestampRange{Start: current + 1, End: next}, nil
		}
		current = oracle.next.Load()
	}
}

// Observe advances the oracle when a timestamp from another source is newer.
// Stale and equal observations are no-ops. It returns true only when state
// advances.
func (oracle *SQLLogicalTimestampOracle) Observe(timestamp uint64) bool {
	if oracle == nil {
		return false
	}
	current := oracle.next.Load()
	for timestamp > current {
		if oracle.next.CompareAndSwap(current, timestamp) {
			return true
		}
		current = oracle.next.Load()
	}
	return false
}
