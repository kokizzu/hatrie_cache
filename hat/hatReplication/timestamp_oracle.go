package hatReplication

import (
	"errors"
	"sync/atomic"
)

const timestampOracleMax int64 = 1<<63 - 1

var (
	// ErrTimestampOracleInvalid reports a nil oracle or negative timestamp.
	ErrTimestampOracleInvalid = errors.New("hatriecache: timestamp oracle input is invalid")
	// ErrTimestampOracleOverflow reports that no larger timestamp is available.
	ErrTimestampOracleOverflow = errors.New("hatriecache: timestamp oracle overflow")
)

// TimestampOracle is a process-local, monotone Lamport-style timestamp
// authority. It provides a linearizable sequence for local writers and can
// incorporate timestamps observed from other writers. It is not a global
// cross-node ordering service; callers still need a coordinator and a stable
// writer tie-breaker for distributed total ordering.
type TimestampOracle struct {
	current atomic.Int64
}

// NewTimestampOracle creates an oracle whose next timestamp is greater than
// initial. Initial must be nonnegative so zero-value construction remains the
// useful empty-clock state.
func NewTimestampOracle(initial int64) (*TimestampOracle, error) {
	if initial < 0 {
		return nil, ErrTimestampOracleInvalid
	}
	oracle := &TimestampOracle{}
	oracle.current.Store(initial)
	return oracle, nil
}

// Current returns the greatest timestamp allocated or observed so far. A nil
// oracle reports zero.
func (oracle *TimestampOracle) Current() int64 {
	if oracle == nil {
		return 0
	}
	return oracle.current.Load()
}

// Next allocates one timestamp greater than every timestamp previously
// allocated or observed. Concurrent calls receive unique timestamps.
func (oracle *TimestampOracle) Next() (int64, error) {
	if oracle == nil {
		return 0, ErrTimestampOracleInvalid
	}
	for {
		current := oracle.current.Load()
		if current == timestampOracleMax {
			return 0, ErrTimestampOracleOverflow
		}
		if oracle.current.CompareAndSwap(current, current+1) {
			return current + 1, nil
		}
	}
}

// Observe advances the oracle to timestamp when timestamp is newer. Older or
// equal observations are ignored. A later Next call is therefore greater than
// every timestamp observed by this oracle.
func (oracle *TimestampOracle) Observe(timestamp int64) error {
	if oracle == nil || timestamp < 0 {
		return ErrTimestampOracleInvalid
	}
	for {
		current := oracle.current.Load()
		if timestamp <= current {
			return nil
		}
		if oracle.current.CompareAndSwap(current, timestamp) {
			return nil
		}
	}
}
