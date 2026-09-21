package hatSql

import (
	"errors"
	"fmt"
)

var (
	// ErrSQLFrontierBeforeSince means the requested frontier is older than the
	// history retained by the source.
	ErrSQLFrontierBeforeSince = errors.New("hatSql: SQL frontier is before the retained since frontier")
	// ErrSQLFrontierAtOrAfterUpper means the requested frontier is not a valid
	// point in the source's half-open retained interval.
	ErrSQLFrontierAtOrAfterUpper = errors.New("hatSql: SQL frontier is at or after the retained upper frontier")
	// ErrSQLFrontierBoundsInvalid means a provider returned an invalid interval.
	ErrSQLFrontierBoundsInvalid = errors.New("hatSql: SQL frontier bounds are invalid")
)

// SQLFrontierBounds describes the half-open interval [Since, Upper) for which
// a source can open an exact historical SQL snapshot.
type SQLFrontierBounds struct {
	Since uint64
	Upper uint64
}

// Validate rejects frontiers outside the retained interval without asking a
// storage provider to construct a partial or ambiguous snapshot.
func (bounds SQLFrontierBounds) Validate(frontier uint64) error {
	if bounds.Upper <= bounds.Since {
		return fmt.Errorf("%w: since=%d upper=%d", ErrSQLFrontierBoundsInvalid, bounds.Since, bounds.Upper)
	}
	if frontier < bounds.Since {
		return fmt.Errorf("%w: frontier=%d since=%d", ErrSQLFrontierBeforeSince, frontier, bounds.Since)
	}
	if frontier >= bounds.Upper {
		return fmt.Errorf("%w: frontier=%d upper=%d", ErrSQLFrontierAtOrAfterUpper, frontier, bounds.Upper)
	}
	return nil
}

// SQLFrontierBoundsProvider optionally exposes the exact retained interval of
// a SQL frontier snapshot provider. Providers without this capability retain
// the legacy behavior and are treated as unbounded by SQL's common entrypoints.
type SQLFrontierBoundsProvider interface {
	SQLFrontierBounds() (SQLFrontierBounds, error)
}

func validateSQLFrontierBounds(resolver interface{}, frontier uint64) error {
	provider, ok := resolver.(SQLFrontierBoundsProvider)
	if !ok {
		return nil
	}
	bounds, err := provider.SQLFrontierBounds()
	if err != nil {
		return err
	}
	return validateSQLFrontierBoundsValue(provider, bounds, frontier)
}

func validateSQLFrontierBoundsValue(provider interface{}, bounds SQLFrontierBounds, frontier uint64) error {
	err := bounds.Validate(frontier)
	if err == nil || !errors.Is(err, ErrSQLFrontierBeforeSince) || bounds.Since == 0 {
		return err
	}
	switch provider.(type) {
	case *TypedTable, *TypedTableSQLSnapshotRegistry:
		// Keep the pre-M211 compaction identity discoverable while exposing the
		// explicit lower-bound classification to newer callers.
		return fmt.Errorf("%w: %w", err, ErrTypedTableMVCCCompacted)
	}
	return err
}
