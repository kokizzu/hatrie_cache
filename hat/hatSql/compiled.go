package hatSql

import (
	"context"
	"fmt"
	"sync"
)

// CompiledSQLQuery is an immutable parsed SQL template. It is safe for
// concurrent execution. Static default executions reuse the immutable template;
// parameterized or execution-local options use a per-call clone.
type CompiledSQLQuery struct {
	source        string
	template      *sqlQuery
	hasParameters bool
	dataflowPlan  *SQLDataflowPlan
	dataflowOnce  sync.Once
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
	// Compile-time rewrites are safe because the template is immutable after
	// construction. Parameterized executions still clone and rewrite after
	// binding so parameter-dependent folds remain execution-local.
	rewriteSQLQuery(template)
	applySQLQueryCollation(template, SQLCollationBinary)
	return &CompiledSQLQuery{
		source:        source,
		template:      template,
		hasParameters: sqlQueryHasParameters(template),
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
	options.compiledTemplateReadOnly = query.readOnlyTemplateEligible(parameters, options)
	return ExecuteSQLQueryParameters(ctx, query.source, resolver, parameters, options)
}

// ExecuteRows binds positional values and streams each compatible result row
// from the compiled query without materializing the result set.
func (query *CompiledSQLQuery) ExecuteRows(ctx context.Context, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, visit func([]string, SQLRow) error) error {
	if query == nil || query.template == nil {
		return fmt.Errorf("compiled SQL query is required")
	}
	options.compiledTemplate = query.template
	options.compiledTemplateReadOnly = query.readOnlyTemplateEligible(parameters, options)
	return ExecuteSQLQueryRows(ctx, query.source, resolver, parameters, options, visit)
}

func (query *CompiledSQLQuery) readOnlyTemplateEligible(parameters []interface{}, options SQLQueryOptions) bool {
	if query == nil || query.template == nil || query.hasParameters || len(parameters) != 0 {
		return false
	}
	if options.Collation != "" || options.Optimizer != nil {
		return false
	}
	return options.IndexHint.Source == "" && options.IndexHint.Field == "" && options.IndexHint.Mode == ""
}

func sqlQueryHasParameters(query *sqlQuery) bool {
	if query == nil {
		return false
	}
	for _, cte := range query.ctes {
		if sqlQueryHasParameters(cte.query) || sqlValuesHaveParameters(cte.values) {
			return true
		}
	}
	if sqlSourceHasParameters(query.from) {
		return true
	}
	for _, join := range query.joins {
		if sqlSourceHasParameters(&join.source) || sqlExprHasParameters(join.on) {
			return true
		}
	}
	for _, item := range query.selects {
		if sqlExprHasParameters(item.expr) {
			return true
		}
	}
	if sqlExprHasParameters(query.where) || sqlExprHasParameters(query.having) {
		return true
	}
	for _, expression := range query.groupBy {
		if sqlExprHasParameters(expression) {
			return true
		}
	}
	for _, groupingSet := range query.groupingSets {
		for _, expression := range groupingSet {
			if sqlExprHasParameters(expression) {
				return true
			}
		}
	}
	for _, expression := range query.groupingDimensions {
		if sqlExprHasParameters(expression) {
			return true
		}
	}
	for _, order := range query.orderBy {
		if sqlExprHasParameters(order.expr) {
			return true
		}
	}
	for _, window := range query.windows {
		for _, expression := range window.partition {
			if sqlExprHasParameters(expression) {
				return true
			}
		}
		for _, order := range window.order {
			if sqlExprHasParameters(order.expr) {
				return true
			}
		}
	}
	if query.limitBy != nil {
		for _, expression := range query.limitBy.expressions {
			if sqlExprHasParameters(expression) {
				return true
			}
		}
	}
	for _, union := range query.unions {
		if sqlQueryHasParameters(union.query) {
			return true
		}
	}
	return false
}

func sqlSourceHasParameters(source *sqlSource) bool {
	return source != nil && (source.keyParameter != 0 || sqlValuesHaveParameters(source.values) || sqlQueryHasParameters(source.query))
}

func sqlValuesHaveParameters(values [][]interface{}) bool {
	for _, row := range values {
		for _, value := range row {
			if _, ok := value.(sqlParameter); ok {
				return true
			}
		}
	}
	return false
}

func sqlExprHasParameters(expression sqlExpr) bool {
	if expression.kind == "parameter" {
		return true
	}
	if expression.query != nil && sqlQueryHasParameters(expression.query) {
		return true
	}
	if expression.left != nil && sqlExprHasParameters(*expression.left) {
		return true
	}
	if expression.right != nil && sqlExprHasParameters(*expression.right) {
		return true
	}
	if expression.filter != nil && sqlExprHasParameters(*expression.filter) {
		return true
	}
	for _, argument := range expression.args {
		if sqlExprHasParameters(argument) {
			return true
		}
	}
	for _, branch := range expression.cases {
		if sqlExprHasParameters(branch.when) || sqlExprHasParameters(branch.then) {
			return true
		}
	}
	if expression.window != nil {
		for _, partition := range expression.window.partition {
			if sqlExprHasParameters(partition) {
				return true
			}
		}
		for _, order := range expression.window.order {
			if sqlExprHasParameters(order.expr) {
				return true
			}
		}
	}
	return false
}
