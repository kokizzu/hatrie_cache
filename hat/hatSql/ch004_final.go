package hatSql

import "errors"

// SQLFinalMode selects the version reconciliation performed by a FINAL source.
type SQLFinalMode uint8

const (
	// SQLFinalReplacing keeps the newest row for each logical key.
	SQLFinalReplacing SQLFinalMode = iota + 1
	// SQLFinalCollapsing cancels adjacent logical updates with opposite signs.
	SQLFinalCollapsing
)

var (
	// ErrSQLFinalOptionsRequired means a query requested FINAL without an
	// explicit source reconciliation contract.
	ErrSQLFinalOptionsRequired = errors.New("hatSql: FINAL source options are required")
	// ErrSQLFinalOptionsInvalid means a FINAL contract is incomplete or mixes
	// callbacks that do not belong to its selected mode.
	ErrSQLFinalOptionsInvalid = errors.New("hatSql: invalid FINAL source options")
)

// SQLFinalOptions describes one source's immutable reconciliation contract.
// Callbacks must only inspect their row and must be deterministic. FINAL is
// deliberately callback-configured because a generic SQL row has no safe
// universal primary-key, version, or sign column.
type SQLFinalOptions struct {
	Mode    SQLFinalMode
	Key     SQLReplacingMergeKeyFunc
	Version SQLReplacingMergeVersionFunc
	Sign    SQLCollapsingMergeSignFunc
}

// SQLFinalSourceOptionsFunc resolves the contract for one parsed source.
// Returning false means that source is not configured and causes a FINAL
// query to fail closed instead of silently returning unreconciled rows.
type SQLFinalSourceOptionsFunc func(kind, key string) (SQLFinalOptions, bool, error)

// SQLFinalSourceOptionsResolver keeps the callback behind a pointer so
// SQLQueryOptions remains a comparable value when FINAL is not configured.
type SQLFinalSourceOptionsResolver struct {
	Resolve SQLFinalSourceOptionsFunc
}

func (options SQLFinalOptions) validate() error {
	if options.Key == nil {
		return ErrSQLFinalOptionsInvalid
	}
	switch options.Mode {
	case SQLFinalReplacing:
		if options.Sign != nil {
			return ErrSQLFinalOptionsInvalid
		}
	case SQLFinalCollapsing:
		if options.Sign == nil || options.Version != nil {
			return ErrSQLFinalOptionsInvalid
		}
	default:
		return ErrSQLFinalOptionsInvalid
	}
	return nil
}

func finalizeSQLSourceRows(source sqlSource, control *sqlExecutionControl, rows []SQLRow) ([]SQLRow, error) {
	if !source.final {
		return rows, nil
	}
	if control == nil {
		return nil, ErrSQLFinalOptionsRequired
	}
	var (
		options    SQLFinalOptions
		configured bool
		err        error
	)
	switch {
	case control.options.FinalSourceOptions != nil:
		if control.options.FinalSourceOptions.Resolve == nil {
			return nil, ErrSQLFinalOptionsRequired
		}
		options, configured, err = control.options.FinalSourceOptions.Resolve(source.kind, source.key)
	case control.options.FinalSchema != nil:
		options, configured, err = control.options.FinalSchema.Resolve(source.kind, source.key)
	default:
		return nil, ErrSQLFinalOptionsRequired
	}
	if err != nil {
		return nil, err
	}
	if !configured {
		return nil, ErrSQLFinalOptionsRequired
	}
	if err := options.validate(); err != nil {
		return nil, err
	}
	switch options.Mode {
	case SQLFinalReplacing:
		return ReplaceSQLRows(rows, options.Key, options.Version)
	case SQLFinalCollapsing:
		return CollapseSQLRows(rows, options.Key, options.Sign)
	default:
		return nil, ErrSQLFinalOptionsInvalid
	}
}

func sqlQueryHasFinalSource(query *sqlQuery) bool {
	if query == nil {
		return false
	}
	if query.from != nil && sqlSourceHasFinal(*query.from) {
		return true
	}
	for _, join := range query.joins {
		if sqlSourceHasFinal(join.source) {
			return true
		}
	}
	for _, cte := range query.ctes {
		if sqlQueryHasFinalSource(cte.query) {
			return true
		}
	}
	for _, union := range query.unions {
		if sqlQueryHasFinalSource(union.query) {
			return true
		}
	}
	return false
}

func sqlSourceHasFinal(source sqlSource) bool {
	return source.final || sqlQueryHasFinalSource(source.query)
}
