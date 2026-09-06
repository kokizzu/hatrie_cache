package hatSql

import (
	"fmt"
	"strings"
	"time"
)

// sqlOrderFill stores the literal gap-filling bounds attached to one ORDER BY
// item. The executor resolves the output column after projection so aliases
// remain usable without changing the public FillSQLRows API.
type sqlOrderFill struct {
	spec SQLWithFillSpec
}

func (p *sqlQueryParser) parseSQLWithFillSpec(column string) (SQLWithFillSpec, error) {
	spec := SQLWithFillSpec{Column: column}
	if err := p.expectKeyword("FROM"); err != nil {
		return SQLWithFillSpec{}, err
	}
	from, err := p.parsePrimary()
	if err != nil {
		return SQLWithFillSpec{}, err
	}
	value, ok := from.value.(time.Time)
	if from.kind != "literal" || !ok {
		return SQLWithFillSpec{}, p.diagnostic(p.previous(), "WITH FILL FROM requires a TIMESTAMP literal")
	}
	spec.From = value
	if err := p.expectKeyword("TO"); err != nil {
		return SQLWithFillSpec{}, err
	}
	to, err := p.parsePrimary()
	if err != nil {
		return SQLWithFillSpec{}, err
	}
	value, ok = to.value.(time.Time)
	if to.kind != "literal" || !ok {
		return SQLWithFillSpec{}, p.diagnostic(p.previous(), "WITH FILL TO requires a TIMESTAMP literal")
	}
	spec.To = value
	if err := p.expectKeyword("STEP"); err != nil {
		return SQLWithFillSpec{}, err
	}
	step, err := p.parsePrimary()
	if err != nil {
		return SQLWithFillSpec{}, err
	}
	duration, ok := step.value.(sqlDuration)
	if step.kind != "literal" || !ok {
		return SQLWithFillSpec{}, p.diagnostic(p.previous(), "WITH FILL STEP requires a DURATION literal")
	}
	spec.Step, err = time.ParseDuration(string(duration))
	if err != nil || spec.Step <= 0 {
		return SQLWithFillSpec{}, p.diagnostic(p.previous(), "WITH FILL STEP requires a positive DURATION literal")
	}
	if spec.From.IsZero() || spec.To.IsZero() || !spec.To.After(spec.From) {
		return SQLWithFillSpec{}, p.diagnostic(p.previous(), "WITH FILL requires non-zero bounds with TO after FROM")
	}
	return spec, nil
}

func sqlQueryHasWithFill(query *sqlQuery) bool {
	if query == nil {
		return false
	}
	for _, order := range query.orderBy {
		if order.fill != nil {
			return true
		}
	}
	for _, cte := range query.ctes {
		if sqlQueryHasWithFill(cte.query) {
			return true
		}
	}
	for _, union := range query.unions {
		if sqlQueryHasWithFill(union.query) {
			return true
		}
	}
	return false
}

func sqlWithFillSpecForQuery(query *sqlQuery, columns []string) (SQLWithFillSpec, bool, error) {
	if query == nil {
		return SQLWithFillSpec{}, false, nil
	}
	var fill *sqlOrderFill
	for index := range query.orderBy {
		if query.orderBy[index].fill == nil {
			continue
		}
		if fill != nil || len(query.orderBy) != 1 {
			return SQLWithFillSpec{}, false, fmt.Errorf("%w: exactly one ORDER BY item is supported", ErrSQLWithFillInvalid)
		}
		fill = query.orderBy[index].fill
	}
	if fill == nil {
		return SQLWithFillSpec{}, false, nil
	}
	if query.limitBy != nil {
		return SQLWithFillSpec{}, false, fmt.Errorf("%w: WITH FILL cannot be combined with LIMIT BY", ErrSQLWithFillInvalid)
	}
	spec := fill.spec
	for _, column := range columns {
		if strings.EqualFold(column, spec.Column) {
			spec.Column = column
			return spec, true, nil
		}
	}
	for index, item := range query.selects {
		if index >= len(columns) || item.expr.kind != "field" || item.expr.qualifier != "" || !strings.EqualFold(item.expr.name, spec.Column) {
			continue
		}
		spec.Column = columns[index]
		return spec, true, nil
	}
	return SQLWithFillSpec{}, false, fmt.Errorf("%w: ORDER BY column %q must be selected", ErrSQLWithFillInvalid, spec.Column)
}

func applySQLWithFill(query *sqlQuery, columns []string, rows []SQLRow, maxRows int) ([]SQLRow, error) {
	spec, ok, err := sqlWithFillSpecForQuery(query, columns)
	if err != nil || !ok {
		return rows, err
	}
	spec.Template = make(Row, len(columns))
	for _, column := range columns {
		spec.Template[column] = nil
	}
	return fillSQLRowsBounded(rows, spec, maxRows)
}

func sqlExplainWithFill(spec SQLWithFillSpec) string {
	return fmt.Sprintf(" WITH FILL FROM TIMESTAMP '%s' TO TIMESTAMP '%s' STEP DURATION '%s'", spec.From.Format(time.RFC3339Nano), spec.To.Format(time.RFC3339Nano), spec.Step)
}
