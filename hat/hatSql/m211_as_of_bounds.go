package hatSql

import (
	"errors"
	"fmt"
)

var (
	// ErrSQLAsOfBoundsRequireFrontier reports bounds supplied without a
	// concrete historical frontier to check.
	ErrSQLAsOfBoundsRequireFrontier = errors.New("hatSql: AS OF bounds require a frontier")
	// ErrSQLAsOfBoundsInvalid reports an empty or reversed [since, upper)
	// interval.
	ErrSQLAsOfBoundsInvalid = errors.New("hatSql: AS OF bounds are invalid")
	// ErrSQLAsOfBeforeSince reports a frontier below the inclusive lower bound.
	ErrSQLAsOfBeforeSince = errors.New("hatSql: AS OF frontier is before since")
	// ErrSQLAsOfAtOrAfterUpper reports a frontier at or beyond the exclusive
	// upper bound.
	ErrSQLAsOfAtOrAfterUpper = errors.New("hatSql: AS OF frontier is at or after upper")
)

func (options *SQLQueryOptions) normalizeSQLAsOfBounds() error {
	if options == nil || (options.AsOfSince == nil && options.AsOfUpper == nil) {
		return nil
	}
	if options.AsOfFrontier == nil {
		return ErrSQLAsOfBoundsRequireFrontier
	}
	if options.AsOfSince != nil && options.AsOfUpper != nil && *options.AsOfSince >= *options.AsOfUpper {
		return fmt.Errorf("%w: since=%d upper=%d", ErrSQLAsOfBoundsInvalid, *options.AsOfSince, *options.AsOfUpper)
	}
	if options.AsOfSince != nil && *options.AsOfFrontier < *options.AsOfSince {
		return fmt.Errorf("%w: frontier=%d since=%d", ErrSQLAsOfBeforeSince, *options.AsOfFrontier, *options.AsOfSince)
	}
	if options.AsOfUpper != nil && *options.AsOfFrontier >= *options.AsOfUpper {
		return fmt.Errorf("%w: frontier=%d upper=%d", ErrSQLAsOfAtOrAfterUpper, *options.AsOfFrontier, *options.AsOfUpper)
	}
	return nil
}
