package hatSql

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrSQLLogicalFrontierNil reports a nil logical-frontier receiver.
	ErrSQLLogicalFrontierNil = errors.New("hatSql: SQL logical frontier is nil")
	// ErrSQLLogicalFrontierRegression reports an attempted backwards move.
	ErrSQLLogicalFrontierRegression = errors.New("hatSql: SQL logical frontier moved backwards")
)

// SQLLogicalFrontier is a small concurrency-safe high-watermark for an
// immutable logical timestamp. It stores no rows or history and is opt-in for
// SQL reads and query subscriptions.
type SQLLogicalFrontier struct {
	mu       sync.RWMutex
	frontier uint64
}

// NewSQLLogicalFrontier creates a frontier initialized at value.
func NewSQLLogicalFrontier(value uint64) *SQLLogicalFrontier {
	return &SQLLogicalFrontier{frontier: value}
}

// Current returns the current high-watermark. A nil frontier reports zero.
func (frontier *SQLLogicalFrontier) Current() uint64 {
	if frontier == nil {
		return 0
	}
	frontier.mu.RLock()
	value := frontier.frontier
	frontier.mu.RUnlock()
	return value
}

// Validate checks that value is not older than the current high-watermark.
// Equal values are valid and newer values are valid.
func (frontier *SQLLogicalFrontier) Validate(value uint64) error {
	if frontier == nil {
		return ErrSQLLogicalFrontierNil
	}
	frontier.mu.RLock()
	current := frontier.frontier
	frontier.mu.RUnlock()
	if value < current {
		return fmt.Errorf("frontier %d follows current %d: %w", value, current, ErrSQLLogicalFrontierRegression)
	}
	return nil
}

// Advance records value when it is newer than the current high-watermark. It
// returns false for an equal value. Older values are rejected and leave state
// unchanged.
func (frontier *SQLLogicalFrontier) Advance(value uint64) (bool, error) {
	if frontier == nil {
		return false, ErrSQLLogicalFrontierNil
	}
	frontier.mu.Lock()
	defer frontier.mu.Unlock()
	if value < frontier.frontier {
		return false, fmt.Errorf("frontier %d follows current %d: %w", value, frontier.frontier, ErrSQLLogicalFrontierRegression)
	}
	if value == frontier.frontier {
		return false, nil
	}
	frontier.frontier = value
	return true, nil
}

func commitSQLLogicalFrontier(options SQLQueryOptions) error {
	if options.LogicalFrontier == nil || options.AsOfFrontier == nil {
		return nil
	}
	_, err := options.LogicalFrontier.Advance(*options.AsOfFrontier)
	return err
}
