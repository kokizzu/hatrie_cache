package hatSql

import (
	"context"
	"fmt"
	"sync"
)

// CompiledSQLQuery is an immutable parsed SQL template. It is safe for
// concurrent execution; every call clones the template before binding values
// or applying execution-local rewrites.
type CompiledSQLQuery struct {
	source       string
	template     *sqlQuery
	dataflowPlan *SQLDataflowPlan
	dataflowOnce sync.Once
}

// CompileSQLQuery parses a SQL source into an immutable reusable template.
// Positional values are supplied later to Execute or ExecuteRows. The source
// is compiled once here, so repeated executions avoid parser and cache lookup
// work without changing the generic query entry points.
func CompileSQLQuery(source string) (*CompiledSQLQuery, error) {
	compiled, err := CompileSQLShortcut(source)
	if err != nil {
		return nil, err
	}
	template, err := parseSQLQueryTemplate(compiled)
	if err != nil {
		return nil, err
	}
	return &CompiledSQLQuery{
		source:   source,
		template: template,
	}, nil
}

// Source returns the original SQL source used to compile the query.
func (query *CompiledSQLQuery) Source() string {
	if query == nil {
		return ""
	}
	return query.source
}

// Execute binds positional values and evaluates the compiled query.
func (query *CompiledSQLQuery) Execute(ctx context.Context, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	if query == nil || query.template == nil {
		return SQLQueryResult{}, fmt.Errorf("compiled SQL query is required")
	}
	options.compiledTemplate = query.template
	return ExecuteSQLQueryParameters(ctx, query.source, resolver, parameters, options)
}

// ExecuteRows binds positional values and streams each compatible result row
// from the compiled query without materializing the result set.
func (query *CompiledSQLQuery) ExecuteRows(ctx context.Context, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, visit func([]string, SQLRow) error) error {
	if query == nil || query.template == nil {
		return fmt.Errorf("compiled SQL query is required")
	}
	options.compiledTemplate = query.template
	return ExecuteSQLQueryRows(ctx, query.source, resolver, parameters, options, visit)
}
