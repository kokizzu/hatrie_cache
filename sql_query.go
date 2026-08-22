package hatriecache

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const maxSQLQueryRows = 100000

// SQLQueryRequest is accepted by the monitoring SQL endpoint.
type SQLQueryRequest struct {
	Query string `json:"query"`
}

// SQLRow is one dynamically shaped row returned by the read-only SQL query engine.
type SQLRow map[string]interface{}

// SQLQueryResult is a materialized result. Streaming clients use QueryRows.
type SQLQueryResult struct {
	Columns []string         `json:"columns"`
	Rows    []SQLRow         `json:"rows"`
	Plan    []SQLExplainStep `json:"plan,omitempty"`
	Stats   *SQLQueryStats   `json:"stats,omitempty"`
}

// SQLExplainStep is one stable, human-readable operation in an EXPLAIN plan.
// EstimatedRows is present only when the parser can know it without reading a
// cache source (for example an inline VALUES source).
type SQLExplainStep struct {
	Node             string `json:"node"`
	Detail           string `json:"detail"`
	EstimatedRows    *int   `json:"estimated_rows,omitempty"`
	ActualInputRows  *int   `json:"actual_input_rows,omitempty"`
	ActualOutputRows *int   `json:"actual_output_rows,omitempty"`
	ElapsedNanos     *int64 `json:"elapsed_ns,omitempty"`
}

// SQLQueryStats is emitted only by EXPLAIN ANALYZE. It describes one actual
// execution, including its total elapsed time, not an extrapolated estimate.
type SQLQueryStats struct {
	ElapsedNanos  int64 `json:"elapsed_ns"`
	OutputRows    int   `json:"output_rows"`
	OutputColumns int   `json:"output_columns"`
	PlanSteps     int   `json:"plan_steps"`
}

// SQLSourceResolver supplies the two cache-backed relational sources. Returning
// nil rows is equivalent to an empty source.
type SQLSourceResolver interface {
	ResolveSQLSource(name string, key string) ([]SQLRow, error)
}

// SQLSourceResolverFunc adapts a function to SQLSourceResolver.
type SQLSourceResolverFunc func(name string, key string) ([]SQLRow, error)

func (fn SQLSourceResolverFunc) ResolveSQLSource(name string, key string) ([]SQLRow, error) {
	if fn == nil {
		return nil, nil
	}
	return fn(name, key)
}

// ResolveSQLSource exposes a stable, read-only snapshot of cache data to SQL.
// CACHE(key) requires a JSON object or array of JSON objects. KEYS returns the
// same metadata fields exposed by the monitoring entries endpoint.
func (ht *HatTrie) ResolveSQLSource(name string, key string) ([]SQLRow, error) {
	switch name {
	case "KEYS":
		entries := ht.monitoringEntries("")
		rows := make([]SQLRow, 0, len(entries))
		for _, entry := range entries {
			rows = append(rows, SQLRow{"key": entry.Key, "type": entry.Type, "ttl_ms": entry.TTLMillis, "on_disk": entry.OnDisk, "size_bytes": entry.SizeBytes, "value_preview": entry.ValuePreview})
		}
		return rows, nil
	case "CACHE":
		data, err := ht.GetBytesChecked(key)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return []SQLRow{}, nil
		}
		var array []SQLRow
		if err := json.Unmarshal(data, &array); err == nil {
			return array, nil
		}
		var object SQLRow
		if err := json.Unmarshal(data, &object); err == nil {
			return []SQLRow{object}, nil
		}
		return nil, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	default:
		return nil, fmt.Errorf("unknown SQL source %q", name)
	}
}

// ExecuteSQLQuery parses and executes a read-only relational query against a
// snapshot supplied by resolver. It intentionally does not execute cache commands.
func ExecuteSQLQuery(source string, resolver SQLSourceResolver) (SQLQueryResult, error) {
	query, err := parseSQLQuery(source)
	if err != nil {
		return SQLQueryResult{}, err
	}
	if query.explain {
		return explainSQLQuery(query, resolver)
	}
	return executeSQLQuery(query, resolver, nil)
}

// ValidateSQLQuery verifies syntax without reading any cache source.
func ValidateSQLQuery(source string) error { _, err := parseSQLQuery(source); return err }

func parseSQLQuery(source string) (*sqlQuery, error) {
	tokens, err := lexSQL(source)
	if err != nil {
		return nil, err
	}
	parser := sqlQueryParser{tokens: tokens}
	explain := false
	analyze := false
	if parser.keyword("EXPLAIN") {
		explain = true
		parser.next()
		if parser.keyword("ANALYZE") {
			analyze = true
			parser.next()
		}
		if parser.current().kind == sqlTokenEOF || parser.current().kind == sqlTokenSemicolon {
			label := "EXPLAIN"
			if analyze {
				label += " ANALYZE"
			}
			return nil, parser.diagnostic(parser.current(), label+" requires a query after it")
		}
	}
	query, err := parser.parseQuery(false)
	if err != nil {
		return nil, err
	}
	if parser.current().kind == sqlTokenSemicolon {
		parser.next()
	}
	if parser.current().kind != sqlTokenEOF {
		return nil, parser.expected(parser.current(), "end of input", nil)
	}
	query.explain = explain
	query.analyze = analyze
	return query, nil
}

type sqlQuery struct {
	ctes     []sqlCTE
	selects  []sqlSelectItem
	from     *sqlSource
	joins    []sqlJoin
	where    sqlExpr
	groupBy  []sqlExpr
	having   sqlExpr
	orderBy  []sqlOrder
	limit    int
	offset   int
	distinct bool
	unions   []sqlUnion
	explain  bool
	analyze  bool
}
type sqlUnion struct {
	kind  string
	all   bool
	query *sqlQuery
}
type sqlCTE struct {
	name    string
	columns []string
	query   *sqlQuery
	values  [][]interface{}
}
type sqlSource struct {
	kind, key, alias string
	values           [][]interface{}
	columns          []string
	query            *sqlQuery
}
type sqlJoin struct {
	kind   string
	source sqlSource
	on     sqlExpr
}
type sqlSelectItem struct {
	expr  sqlExpr
	alias string
}
type sqlOrder struct {
	expr sqlExpr
	desc bool
}
type sqlExpr struct {
	kind, name, qualifier, op string
	value                     interface{}
	left, right               *sqlExpr
	args                      []sqlExpr
}

type sqlQueryParser struct {
	tokens []sqlToken
	index  int
}

func (p *sqlQueryParser) parseQuery(stopRight bool) (*sqlQuery, error) {
	q := &sqlQuery{limit: -1}
	if p.keyword("UNION") || p.keyword("INTERSECT") || p.keyword("EXCEPT") {
		return nil, p.expected(p.current(), "SELECT, FROM, or WITH", []string{"SELECT", "FROM", "WITH"})
	}
	if p.keyword("WITH") {
		p.next()
		for {
			name, err := p.expectIdentifier("a CTE name", nil)
			if err != nil {
				return nil, err
			}
			cte := sqlCTE{name: strings.ToUpper(name.text)}
			if p.current().kind == sqlTokenLeftParen {
				cols, err := p.parseColumns()
				if err != nil {
					return nil, err
				}
				cte.columns = cols
			}
			if err := p.expectKeyword("AS"); err != nil {
				return nil, err
			}
			if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
				return nil, err
			}
			if p.keyword("VALUES") {
				values, err := p.parseValues()
				if err != nil {
					return nil, err
				}
				cte.values = values
			} else {
				nested, err := p.parseQuery(true)
				if err != nil {
					return nil, err
				}
				cte.query = nested
			}
			if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
				return nil, err
			}
			q.ctes = append(q.ctes, cte)
			if p.current().kind != sqlTokenComma {
				break
			}
			p.next()
		}
	}
	for p.current().kind != sqlTokenEOF && !(stopRight && p.current().kind == sqlTokenRightParen) && !p.keyword("UNION") && !p.keyword("INTERSECT") && !p.keyword("EXCEPT") {
		switch {
		case p.keyword("SELECT"):
			if q.selects != nil {
				return nil, p.diagnostic(p.current(), "SELECT appears more than once")
			}
			p.next()
			if p.keyword("DISTINCT") {
				q.distinct = true
				p.next()
			}
			items, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			q.selects = items
		case p.keyword("FROM"):
			if q.from != nil {
				return nil, p.diagnostic(p.current(), "FROM appears more than once")
			}
			p.next()
			source, err := p.parseSource()
			if err != nil {
				return nil, err
			}
			q.from = &source
		case p.keyword("JOIN") || p.keyword("INNER") || p.keyword("LEFT") || p.keyword("RIGHT") || p.keyword("FULL") || p.keyword("CROSS"):
			if q.from == nil {
				return nil, p.diagnostic(p.current(), "JOIN requires FROM first")
			}
			join, err := p.parseJoin()
			if err != nil {
				return nil, err
			}
			q.joins = append(q.joins, join)
		case p.keyword("WHERE"):
			if q.where.kind != "" {
				return nil, p.diagnostic(p.current(), "WHERE appears more than once")
			}
			p.next()
			expr, err := p.parseCondition()
			if err != nil {
				return nil, err
			}
			q.where = expr
		case p.keyword("GROUP"):
			if q.groupBy != nil {
				return nil, p.diagnostic(p.current(), "GROUP BY appears more than once")
			}
			p.next()
			if err := p.expectKeyword("BY"); err != nil {
				return nil, err
			}
			values, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			q.groupBy = values
		case p.keyword("HAVING"):
			if q.having.kind != "" {
				return nil, p.diagnostic(p.current(), "HAVING appears more than once")
			}
			p.next()
			expr, err := p.parseCondition()
			if err != nil {
				return nil, err
			}
			q.having = expr
		case p.keyword("ORDER"):
			if q.orderBy != nil {
				return nil, p.diagnostic(p.current(), "ORDER BY appears more than once")
			}
			p.next()
			if err := p.expectKeyword("BY"); err != nil {
				return nil, err
			}
			order, err := p.parseOrder()
			if err != nil {
				return nil, err
			}
			q.orderBy = order
		case p.keyword("LIMIT"):
			if q.limit >= 0 {
				return nil, p.diagnostic(p.current(), "LIMIT appears more than once")
			}
			p.next()
			value, err := p.parseInteger("LIMIT")
			if err != nil {
				return nil, err
			}
			q.limit = value
		case p.keyword("OFFSET"):
			if q.offset != 0 {
				return nil, p.diagnostic(p.current(), "OFFSET appears more than once")
			}
			p.next()
			value, err := p.parseInteger("OFFSET")
			if err != nil {
				return nil, err
			}
			q.offset = value
		default:
			if strings.EqualFold(p.current().text, "JION") {
				return nil, p.expected(p.current(), "JOIN", []string{"JOIN"})
			}
			return nil, p.expected(p.current(), "SELECT, FROM, JOIN, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, or OFFSET", []string{"SELECT", "FROM", "JOIN", "LEFT", "CROSS", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET"})
		}
	}
	if q.from == nil {
		return nil, p.diagnostic(p.current(), "query requires FROM")
	}
	if q.selects == nil {
		return nil, p.diagnostic(p.current(), "query requires SELECT")
	}
	if p.keyword("UNION") || p.keyword("INTERSECT") || p.keyword("EXCEPT") {
		kind := strings.ToUpper(p.current().text)
		p.next()
		union := sqlUnion{kind: kind}
		if p.keyword("ALL") {
			if kind != "UNION" {
				return nil, p.diagnostic(p.current(), kind+" ALL is not supported")
			}
			union.all = true
			p.next()
		}
		if p.current().kind == sqlTokenEOF || p.current().kind == sqlTokenSemicolon || stopRight && p.current().kind == sqlTokenRightParen {
			label := kind
			if union.all {
				label += " ALL"
			}
			return nil, p.diagnostic(p.current(), label+" requires a query after it")
		}
		right, err := p.parseQuery(stopRight)
		if err != nil {
			return nil, err
		}
		union.query = right
		q.unions = append(q.unions, union)
	}
	return q, nil
}

func (p *sqlQueryParser) parseColumns() ([]string, error) {
	p.next()
	var out []string
	for {
		tok, err := p.expectIdentifier("a column name", nil)
		if err != nil {
			return nil, err
		}
		out = append(out, tok.text)
		if p.current().kind != sqlTokenComma {
			break
		}
		p.next()
	}
	if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
		return nil, err
	}
	return out, nil
}
func (p *sqlQueryParser) parseValues() ([][]interface{}, error) {
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	var rows [][]interface{}
	for {
		if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
			return nil, err
		}
		var row []interface{}
		for {
			expr, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			if expr.kind != "literal" {
				return nil, p.diagnostic(p.previous(), "VALUES accepts literals only")
			}
			row = append(row, expr.value)
			if p.current().kind != sqlTokenComma {
				break
			}
			p.next()
		}
		if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
			return nil, err
		}
		if len(rows) > 0 && len(rows[0]) != len(row) {
			return nil, p.diagnostic(p.previous(), "all VALUES rows must have the same number of columns")
		}
		rows = append(rows, row)
		if p.current().kind != sqlTokenComma || p.peek().kind != sqlTokenLeftParen {
			break
		}
		p.next()
	}
	return rows, nil
}
func (p *sqlQueryParser) parseSource() (sqlSource, error) {
	if p.current().kind == sqlTokenLeftParen {
		p.next()
		query, err := p.parseQuery(true)
		if err != nil {
			return sqlSource{}, err
		}
		if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
			return sqlSource{}, err
		}
		source := sqlSource{kind: "SUBQUERY", query: query}
		p.parseAlias(&source)
		return source, nil
	}
	if p.keyword("VALUES") {
		rows, err := p.parseValues()
		if err != nil {
			return sqlSource{}, err
		}
		source := sqlSource{kind: "VALUES", values: rows}
		p.parseAlias(&source)
		return source, nil
	}
	if p.keyword("CACHE") {
		p.next()
		if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
			return sqlSource{}, err
		}
		value, err := p.parsePrimary()
		if err != nil {
			return sqlSource{}, err
		}
		if value.kind != "literal" || p.current().kind != sqlTokenRightParen {
			return sqlSource{}, p.diagnostic(p.current(), "CACHE requires one literal cache key")
		}
		p.next()
		source := sqlSource{kind: "CACHE", key: fmt.Sprint(value.value)}
		p.parseAlias(&source)
		return source, nil
	}
	if p.keyword("KEYS") {
		p.next()
		source := sqlSource{kind: "KEYS"}
		p.parseAlias(&source)
		return source, nil
	}
	name, err := p.expectIdentifier("a source name", nil)
	if err != nil {
		return sqlSource{}, err
	}
	source := sqlSource{kind: "CTE", key: strings.ToUpper(name.text)}
	p.parseAlias(&source)
	return source, nil
}
func (p *sqlQueryParser) parseAlias(source *sqlSource) {
	if p.keyword("AS") {
		p.next()
		if p.current().kind == sqlTokenIdentifier {
			source.alias = p.current().text
			p.next()
		}
	} else if p.current().kind == sqlTokenIdentifier && !sqlClauseKeyword(p.current().text) && !strings.EqualFold(p.current().text, "JION") {
		source.alias = p.current().text
		p.next()
	}
	if p.current().kind == sqlTokenLeftParen {
		cols, _ := p.parseColumns()
		source.columns = cols
	}
	if source.alias == "" {
		if source.kind == "CTE" {
			source.alias = strings.ToLower(source.key)
		} else {
			source.alias = strings.ToLower(source.kind)
		}
	}
}
func (p *sqlQueryParser) parseJoin() (sqlJoin, error) {
	kind := "INNER"
	token := p.current()
	if p.keyword("INNER") {
		p.next()
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	} else if p.keyword("LEFT") {
		kind = "LEFT"
		p.next()
		if p.keyword("OUTER") {
			p.next()
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	} else if p.keyword("RIGHT") {
		kind = "RIGHT"
		p.next()
		if p.keyword("OUTER") {
			p.next()
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	} else if p.keyword("FULL") {
		kind = "FULL"
		p.next()
		if p.keyword("OUTER") {
			p.next()
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	} else if p.keyword("CROSS") {
		kind = "CROSS"
		p.next()
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	} else {
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	}
	source, err := p.parseSource()
	if err != nil {
		return sqlJoin{}, err
	}
	join := sqlJoin{kind: kind, source: source}
	if kind != "CROSS" {
		if err := p.expectKeyword("ON"); err != nil {
			return sqlJoin{}, err
		}
		on, err := p.parseCondition()
		if err != nil {
			return sqlJoin{}, err
		}
		join.on = on
	}
	_ = token
	return join, nil
}
func (p *sqlQueryParser) parseSelect() ([]sqlSelectItem, error) {
	var out []sqlSelectItem
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		item := sqlSelectItem{expr: expr}
		if p.keyword("AS") {
			p.next()
			alias, err := p.expectIdentifier("an alias", nil)
			if err != nil {
				return nil, err
			}
			item.alias = alias.text
		} else if p.current().kind == sqlTokenIdentifier && !sqlClauseKeyword(p.current().text) && !sqlSuspectedClauseTypo(p.current().text) {
			item.alias = p.current().text
			p.next()
		}
		out = append(out, item)
		if p.current().kind != sqlTokenComma {
			break
		}
		p.next()
	}
	return out, nil
}
func (p *sqlQueryParser) parseExprList() ([]sqlExpr, error) {
	var out []sqlExpr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		out = append(out, expr)
		if p.current().kind != sqlTokenComma {
			break
		}
		p.next()
	}
	return out, nil
}
func (p *sqlQueryParser) parseOrder() ([]sqlOrder, error) {
	var out []sqlOrder
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		value := sqlOrder{expr: expr}
		if p.keyword("ASC") {
			p.next()
		} else if p.keyword("DESC") {
			p.next()
			value.desc = true
		}
		out = append(out, value)
		if p.current().kind != sqlTokenComma {
			break
		}
		p.next()
	}
	return out, nil
}
func (p *sqlQueryParser) parseCondition() (sqlExpr, error) {
	return p.parseOrCondition()
}

// SQL evaluates AND before OR. Keeping this split also makes later support for
// parenthesized predicates unambiguous.
func (p *sqlQueryParser) parseOrCondition() (sqlExpr, error) {
	left, err := p.parseAndCondition()
	if err != nil {
		return sqlExpr{}, err
	}
	for p.keyword("OR") {
		op := "OR"
		p.next()
		right, err := p.parseAndCondition()
		if err != nil {
			return sqlExpr{}, err
		}
		l := left
		left = sqlExpr{kind: "binary", op: op, left: &l, right: &right}
	}
	return left, nil
}

func (p *sqlQueryParser) parseAndCondition() (sqlExpr, error) {
	left, err := p.parseNotCondition()
	if err != nil {
		return sqlExpr{}, err
	}
	for p.keyword("AND") {
		p.next()
		right, err := p.parseNotCondition()
		if err != nil {
			return sqlExpr{}, err
		}
		previous := left
		left = sqlExpr{kind: "binary", op: "AND", left: &previous, right: &right}
	}
	return left, nil
}

func (p *sqlQueryParser) parseNotCondition() (sqlExpr, error) {
	if p.keyword("NOT") {
		p.next()
		operand, err := p.parseNotCondition()
		if err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "unary", op: "!", left: &operand}, nil
	}
	return p.parseComparison()
}
func (p *sqlQueryParser) parseComparison() (sqlExpr, error) {
	left, err := p.parseExpr()
	if err != nil {
		return sqlExpr{}, err
	}
	if p.keyword("IS") {
		p.next()
		not := false
		if p.keyword("NOT") {
			not = true
			p.next()
		}
		if err := p.expectKeyword("NULL"); err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "binary", op: map[bool]string{false: "IS NULL", true: "IS NOT NULL"}[not], left: &left}, nil
	}
	if p.keyword("LIKE") {
		p.next()
		right, err := p.parseExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "binary", op: "LIKE", left: &left, right: &right}, nil
	}
	switch p.current().kind {
	case sqlTokenEqual, sqlTokenNotEqual, sqlTokenLess, sqlTokenLessEqual, sqlTokenGreater, sqlTokenGreaterEqual:
		op := p.current().text
		p.next()
		right, err := p.parseExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "binary", op: op, left: &left, right: &right}, nil
	}
	return left, nil
}
func (p *sqlQueryParser) parseExpr() (sqlExpr, error) { return p.parseAdditiveExpr() }

func (p *sqlQueryParser) parseAdditiveExpr() (sqlExpr, error) {
	left, err := p.parseMultiplicativeExpr()
	if err != nil {
		return sqlExpr{}, err
	}
	for p.current().kind == sqlTokenPlus || p.current().kind == sqlTokenMinus {
		op := p.current().text
		p.next()
		right, err := p.parseMultiplicativeExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		previous := left
		left = sqlExpr{kind: "binary", op: op, left: &previous, right: &right}
	}
	return left, nil
}

func (p *sqlQueryParser) parseMultiplicativeExpr() (sqlExpr, error) {
	left, err := p.parseUnaryExpr()
	if err != nil {
		return sqlExpr{}, err
	}
	for p.current().kind == sqlTokenStar || p.current().kind == sqlTokenSlash || p.current().kind == sqlTokenPercent {
		op := p.current().text
		p.next()
		right, err := p.parseUnaryExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		previous := left
		left = sqlExpr{kind: "binary", op: op, left: &previous, right: &right}
	}
	return left, nil
}

func (p *sqlQueryParser) parseUnaryExpr() (sqlExpr, error) {
	if p.current().kind == sqlTokenBang || p.current().kind == sqlTokenMinus {
		token := p.current()
		p.next()
		operand, err := p.parseUnaryExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "unary", op: token.text, left: &operand}, nil
	}
	return p.parsePrimary()
}

func (p *sqlQueryParser) parsePrimary() (sqlExpr, error) {
	token := p.current()
	if token.kind == sqlTokenLeftParen {
		p.next()
		expression, err := p.parseCondition()
		if err != nil {
			return sqlExpr{}, err
		}
		if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
			return sqlExpr{}, err
		}
		return expression, nil
	}
	if token.kind == sqlTokenStar {
		p.next()
		return sqlExpr{kind: "star"}, nil
	}
	if token.kind == sqlTokenString {
		p.next()
		return sqlExpr{kind: "literal", value: token.text}, nil
	}
	if token.kind == sqlTokenNumber {
		p.next()
		if strings.ContainsAny(token.text, ".eE") {
			v, e := strconv.ParseFloat(token.text, 64)
			if e != nil {
				return sqlExpr{}, p.diagnostic(token, "invalid number")
			}
			return sqlExpr{kind: "literal", value: v}, nil
		}
		v, e := strconv.ParseInt(token.text, 10, 64)
		if e != nil {
			return sqlExpr{}, p.diagnostic(token, "invalid integer")
		}
		return sqlExpr{kind: "literal", value: v}, nil
	}
	if token.kind == sqlTokenIdentifier {
		p.next()
		upper := strings.ToUpper(token.text)
		if upper == "NULL" {
			return sqlExpr{kind: "literal", value: nil}, nil
		}
		if upper == "TRUE" {
			return sqlExpr{kind: "literal", value: true}, nil
		}
		if upper == "FALSE" {
			return sqlExpr{kind: "literal", value: false}, nil
		}
		if p.current().kind == sqlTokenLeftParen {
			p.next()
			var args []sqlExpr
			if p.current().kind != sqlTokenRightParen {
				for {
					arg, err := p.parseExpr()
					if err != nil {
						return sqlExpr{}, err
					}
					args = append(args, arg)
					if p.current().kind != sqlTokenComma {
						break
					}
					p.next()
				}
			}
			if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
				return sqlExpr{}, err
			}
			return sqlExpr{kind: "func", name: upper, args: args}, nil
		}
		expr := sqlExpr{kind: "field", name: token.text}
		if p.current().kind == sqlTokenDot {
			p.next()
			name, err := p.expectIdentifier("a field name", nil)
			if err != nil {
				return sqlExpr{}, err
			}
			expr.qualifier = token.text
			expr.name = name.text
		}
		return expr, nil
	}
	return sqlExpr{}, p.expected(token, "a column, literal, function, or *", nil)
}
func (p *sqlQueryParser) parseInteger(name string) (int, error) {
	token := p.current()
	if token.kind != sqlTokenNumber {
		return 0, p.expected(token, name+" integer", nil)
	}
	p.next()
	value, err := strconv.Atoi(token.text)
	if err != nil || value < 0 {
		return 0, p.diagnostic(token, name+" must be a non-negative integer")
	}
	return value, nil
}
func (p *sqlQueryParser) current() sqlToken {
	if p.index >= len(p.tokens) {
		return sqlToken{kind: sqlTokenEOF, line: 1, column: 1, endColumn: 1}
	}
	return p.tokens[p.index]
}
func (p *sqlQueryParser) peek() sqlToken {
	if p.index+1 >= len(p.tokens) {
		return sqlToken{kind: sqlTokenEOF}
	}
	return p.tokens[p.index+1]
}
func (p *sqlQueryParser) previous() sqlToken {
	if p.index == 0 {
		return p.current()
	}
	return p.tokens[p.index-1]
}
func (p *sqlQueryParser) next() { p.index++ }
func (p *sqlQueryParser) keyword(word string) bool {
	return p.current().kind == sqlTokenIdentifier && strings.EqualFold(p.current().text, word)
}
func (p *sqlQueryParser) expectKeyword(word string) error {
	if p.keyword(word) {
		p.next()
		return nil
	}
	return p.expected(p.current(), word, []string{word})
}
func (p *sqlQueryParser) expectIdentifier(expected string, candidates []string) (sqlToken, error) {
	if p.current().kind != sqlTokenIdentifier {
		return sqlToken{}, p.expected(p.current(), expected, candidates)
	}
	v := p.current()
	p.next()
	return v, nil
}
func (p *sqlQueryParser) expectKind(kind sqlTokenKind, expected string) error {
	if p.current().kind == kind {
		p.next()
		return nil
	}
	return p.expected(p.current(), expected, nil)
}
func (p *sqlQueryParser) expected(token sqlToken, expected string, candidates []string) error {
	s := ""
	if token.kind == sqlTokenIdentifier {
		s = nearestSQLName(token.text, candidates)
	}
	return &SQLDiagnostic{Message: "unexpected " + token.display() + "; expected " + expected, Line: token.line, Column: token.column, EndColumn: token.endColumn, Suggestion: s}
}
func (p *sqlQueryParser) diagnostic(token sqlToken, message string) error {
	return sqlTokenDiagnostic(token, message)
}
func sqlClauseKeyword(value string) bool {
	switch strings.ToUpper(value) {
	case "EXPLAIN", "ANALYZE", "SELECT", "DISTINCT", "FROM", "JOIN", "LEFT", "RIGHT", "FULL", "CROSS", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "ON", "AS", "INNER", "OUTER", "ASC", "DESC", "UNION", "INTERSECT", "EXCEPT", "ALL":
		return true
	}
	return false
}
func sqlSuspectedClauseTypo(value string) bool {
	return nearestSQLName(value, []string{"SELECT", "FROM", "JOIN", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET"}) != ""
}

type sqlExecRow struct {
	sources map[string]SQLRow
	order   []string
}

func executeSQLQuery(q *sqlQuery, resolver SQLSourceResolver, ctes map[string][]SQLRow) (SQLQueryResult, error) {
	return executeSQLQueryWithMetrics(q, resolver, ctes, nil)
}

type sqlExecutionMetrics struct {
	steps []SQLExplainStep
}

func (metrics *sqlExecutionMetrics) record(node, detail string, inputRows, outputRows int, started time.Time) {
	if metrics == nil {
		return
	}
	elapsed := time.Since(started).Nanoseconds()
	metrics.steps = append(metrics.steps, SQLExplainStep{
		Node:             node,
		Detail:           detail,
		ActualInputRows:  sqlExplainIntPointer(inputRows),
		ActualOutputRows: sqlExplainIntPointer(outputRows),
		ElapsedNanos:     &elapsed,
	})
}

func sqlExplainIntPointer(value int) *int { return &value }

func (metrics *sqlExecutionMetrics) recordScan(source sqlSource, outputRows int, started time.Time) {
	metrics.record("SCAN", sqlExplainSource(source), 0, outputRows, started)
	if metrics == nil || source.kind != "VALUES" {
		return
	}
	metrics.steps[len(metrics.steps)-1].EstimatedRows = sqlExplainIntPointer(len(source.values))
}

func executeSQLQueryWithMetrics(q *sqlQuery, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics) (SQLQueryResult, error) {
	if ctes == nil {
		ctes = map[string][]SQLRow{}
	}
	for _, cte := range q.ctes {
		var rows []SQLRow
		var err error
		if cte.query != nil {
			r, e := executeSQLQueryWithMetrics(cte.query, resolver, ctes, metrics)
			err = e
			rows = r.Rows
		} else {
			rows = valuesSQLRows(cte.values, cte.columns)
		}
		if err != nil {
			return SQLQueryResult{}, err
		}
		ctes[cte.name] = rows
	}
	started := time.Now()
	base, err := resolveSQLSource(*q.from, resolver, ctes, metrics)
	if err != nil {
		return SQLQueryResult{}, err
	}
	if len(base) > maxSQLQueryRows {
		return SQLQueryResult{}, fmt.Errorf("SQL source %q exceeds the %d row limit", q.from.alias, maxSQLQueryRows)
	}
	metrics.recordScan(*q.from, len(base), started)
	rows := wrapSQLSource(*q.from, base)
	leftAliases := []string{q.from.alias}
	for _, join := range q.joins {
		started = time.Now()
		right, err := resolveSQLSource(join.source, resolver, ctes, metrics)
		if err != nil {
			return SQLQueryResult{}, err
		}
		if len(right) > maxSQLQueryRows {
			return SQLQueryResult{}, fmt.Errorf("SQL source %q exceeds the %d row limit", join.source.alias, maxSQLQueryRows)
		}
		wrapped := wrapSQLSource(join.source, right)
		inputRows := len(rows) + len(wrapped)
		var next []sqlExecRow
		matchedRight := make([]bool, len(wrapped))
		for _, left := range rows {
			matched := false
			for rightIndex, r := range wrapped {
				combined := mergeSQLRows(left, r)
				ok := join.kind == "CROSS" || sqlTruthy(evalSQLExpr(join.on, []sqlExecRow{combined}, combined))
				if ok {
					matched = true
					matchedRight[rightIndex] = true
					next = append(next, combined)
					if len(next) > maxSQLQueryRows {
						return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxSQLQueryRows)
					}
				}
			}
			if (join.kind == "LEFT" || join.kind == "FULL") && !matched {
				empty := sqlExecRow{sources: map[string]SQLRow{join.source.alias: {}}, order: []string{join.source.alias}}
				next = append(next, mergeSQLRows(left, empty))
				if len(next) > maxSQLQueryRows {
					return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxSQLQueryRows)
				}
			}
		}
		if join.kind == "RIGHT" || join.kind == "FULL" {
			for rightIndex, right := range wrapped {
				if matchedRight[rightIndex] {
					continue
				}
				emptySources := make(map[string]SQLRow, len(leftAliases))
				for _, alias := range leftAliases {
					emptySources[alias] = SQLRow{}
				}
				emptyLeft := sqlExecRow{sources: emptySources, order: append([]string{}, leftAliases...)}
				next = append(next, mergeSQLRows(emptyLeft, right))
				if len(next) > maxSQLQueryRows {
					return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxSQLQueryRows)
				}
			}
		}
		detail := join.kind + " JOIN " + sqlExplainSource(join.source)
		if join.kind != "CROSS" {
			detail += " ON " + sqlExplainExpression(join.on)
		}
		metrics.record("JOIN", detail, inputRows, len(next), started)
		rows = next
		leftAliases = append(leftAliases, join.source.alias)
	}
	functions, _ := resolver.(SQLFunctionResolver)
	if q.where.kind != "" {
		started = time.Now()
		inputRows := len(rows)
		values, err := evalSQLExprBatch(q.where, rows, functions)
		if err != nil {
			return SQLQueryResult{}, err
		}
		filtered := rows[:0]
		for index, row := range rows {
			if sqlTruthy(values[index]) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
		metrics.record("FILTER", sqlExplainExpression(q.where), inputRows, len(rows), started)
	}
	started = time.Now()
	inputRows := len(rows)
	groups := groupSQLRows(rows, q.groupBy, q)
	if len(q.groupBy) > 0 || sqlQueryHasAggregate(q) {
		metrics.record("AGGREGATE", sqlExplainExpressions(q.groupBy), inputRows, len(groups), started)
	}
	started = time.Now()
	result := SQLQueryResult{Columns: sqlColumns(q.selects), Rows: make([]SQLRow, 0, len(groups))}
	type output struct {
		row   SQLRow
		group []sqlExecRow
	}
	out := make([]output, 0, len(groups))
	for _, group := range groups {
		representative := sqlExecRow{}
		if len(group) > 0 {
			representative = group[0]
		}
		if q.having.kind != "" && !sqlTruthy(evalSQLExpr(q.having, group, representative)) {
			continue
		}
		row := SQLRow{}
		for idx, item := range q.selects {
			if item.expr.kind == "star" {
				for _, source := range representative.order {
					for key, value := range representative.sources[source] {
						row[key] = value
					}
				}
				continue
			}
			row[result.Columns[idx]] = evalSQLExpr(item.expr, group, representative)
		}
		out = append(out, output{row: row, group: group})
	}
	for column, item := range q.selects {
		if !sqlExprHasCustomFunction(item.expr, functions) {
			continue
		}
		calls := make([]sqlExecRow, len(out))
		for index := range out {
			if len(out[index].group) != 1 {
				return SQLQueryResult{}, fmt.Errorf("SQL function %q cannot be combined with grouped or aggregate results", item.expr.name)
			}
			calls[index] = out[index].group[0]
		}
		values, err := evalSQLExprBatch(item.expr, calls, functions)
		if err != nil {
			return SQLQueryResult{}, err
		}
		for index := range out {
			out[index].row[result.Columns[column]] = values[index]
		}
	}
	metrics.record("PROJECT", sqlExplainSelects(q.selects), len(groups), len(out), started)
	if q.distinct {
		started = time.Now()
		inputRows := len(out)
		seen := make(map[string]struct{}, len(out))
		filtered := out[:0]
		for _, item := range out {
			key := sqlOutputRowKey(item.row)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			filtered = append(filtered, item)
		}
		out = filtered
		metrics.record("DISTINCT", "deduplicate projected rows", inputRows, len(out), started)
	}
	if len(q.orderBy) > 0 {
		started = time.Now()
		inputRows := len(out)
		sort.SliceStable(out, func(i, j int) bool {
			for _, item := range q.orderBy {
				a := evalOutputOrder(item.expr, out[i].row, out[i].group)
				b := evalOutputOrder(item.expr, out[j].row, out[j].group)
				cmp := sqlCompare(a, b)
				if cmp != 0 {
					if item.desc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})
		metrics.record("SORT", sqlExplainOrders(q.orderBy), inputRows, len(out), started)
	}
	started = time.Now()
	inputRows = len(out)
	start := q.offset
	if start > len(out) {
		start = len(out)
	}
	end := len(out)
	if q.limit >= 0 && start+q.limit < end {
		end = start + q.limit
	}
	for _, item := range out[start:end] {
		result.Rows = append(result.Rows, item.row)
	}
	if q.limit >= 0 || q.offset > 0 {
		metrics.record("LIMIT", fmt.Sprintf("limit=%d offset=%d", q.limit, q.offset), inputRows, len(result.Rows), started)
	}
	for _, union := range q.unions {
		right, err := executeSQLQueryWithMetrics(union.query, resolver, ctes, metrics)
		if err != nil {
			return SQLQueryResult{}, err
		}
		if !sameSQLColumns(result.Columns, right.Columns) {
			return SQLQueryResult{}, fmt.Errorf("%s queries must project the same column names in the same order", union.kind)
		}
		started = time.Now()
		inputRows = len(result.Rows) + len(right.Rows)
		switch union.kind {
		case "UNION":
			result.Rows = append(result.Rows, right.Rows...)
			if !union.all {
				result.Rows = distinctSQLQueryRows(result.Rows)
			}
		case "INTERSECT":
			available := make(map[string]struct{}, len(right.Rows))
			for _, row := range right.Rows {
				available[sqlOutputRowKey(row)] = struct{}{}
			}
			filtered := result.Rows[:0]
			for _, row := range result.Rows {
				if _, exists := available[sqlOutputRowKey(row)]; exists {
					filtered = append(filtered, row)
				}
			}
			result.Rows = distinctSQLQueryRows(filtered)
		case "EXCEPT":
			excluded := make(map[string]struct{}, len(right.Rows))
			for _, row := range right.Rows {
				excluded[sqlOutputRowKey(row)] = struct{}{}
			}
			filtered := result.Rows[:0]
			for _, row := range result.Rows {
				if _, exists := excluded[sqlOutputRowKey(row)]; !exists {
					filtered = append(filtered, row)
				}
			}
			result.Rows = distinctSQLQueryRows(filtered)
		default:
			return SQLQueryResult{}, fmt.Errorf("unsupported SQL set operation %q", union.kind)
		}
		kind := union.kind
		if union.all {
			kind += " ALL"
		}
		metrics.record("SET", kind, inputRows, len(result.Rows), started)
	}
	return result, nil
}

func explainSQLQuery(query *sqlQuery, resolver SQLSourceResolver) (SQLQueryResult, error) {
	steps := sqlExplainSteps(query)
	result := SQLQueryResult{
		Columns: []string{"node", "detail", "estimated_rows"},
		Rows:    make([]SQLRow, 0, len(steps)+1),
		Plan:    steps,
	}
	for _, step := range steps {
		row := SQLRow{"node": step.Node, "detail": step.Detail}
		if step.EstimatedRows != nil {
			row["estimated_rows"] = *step.EstimatedRows
		}
		result.Rows = append(result.Rows, row)
	}
	if !query.analyze {
		return result, nil
	}
	started := time.Now()
	metrics := &sqlExecutionMetrics{}
	executed, err := executeSQLQueryWithMetrics(query, resolver, nil, metrics)
	if err != nil {
		return SQLQueryResult{}, err
	}
	result.Stats = &SQLQueryStats{
		ElapsedNanos:  time.Since(started).Nanoseconds(),
		OutputRows:    len(executed.Rows),
		OutputColumns: len(executed.Columns),
		PlanSteps:     len(metrics.steps),
	}
	result.Plan = metrics.steps
	result.Rows = result.Rows[:0]
	for _, step := range result.Plan {
		row := SQLRow{"node": step.Node, "detail": step.Detail, "actual_input_rows": *step.ActualInputRows, "actual_output_rows": *step.ActualOutputRows, "elapsed_ns": *step.ElapsedNanos}
		if step.EstimatedRows != nil {
			row["estimated_rows"] = *step.EstimatedRows
		}
		result.Rows = append(result.Rows, row)
	}
	result.Columns = append(result.Columns, "actual_rows", "elapsed_ns")
	result.Rows = append(result.Rows, SQLRow{
		"node":        "ANALYZE",
		"detail":      "execution summary",
		"actual_rows": result.Stats.OutputRows,
		"elapsed_ns":  result.Stats.ElapsedNanos,
	})
	return result, nil
}

func sqlExplainSteps(query *sqlQuery) []SQLExplainStep {
	steps := make([]SQLExplainStep, 0, 8+len(query.ctes)+len(query.joins)+len(query.unions))
	sqlAppendExplainSteps(&steps, query, "")
	return steps
}

func sqlAppendExplainSteps(steps *[]SQLExplainStep, query *sqlQuery, prefix string) {
	for _, cte := range query.ctes {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "CTE", Detail: cte.name})
		if cte.query != nil {
			sqlAppendExplainSteps(steps, cte.query, prefix+"  ")
		} else {
			estimate := len(cte.values)
			*steps = append(*steps, SQLExplainStep{Node: prefix + "  VALUES", Detail: "CTE " + cte.name, EstimatedRows: &estimate})
		}
	}
	*steps = append(*steps, sqlExplainSourceStep(prefix+"SCAN", *query.from))
	if query.from.kind == "SUBQUERY" && query.from.query != nil {
		sqlAppendExplainSteps(steps, query.from.query, prefix+"  ")
	}
	for _, join := range query.joins {
		detail := join.kind + " JOIN " + sqlExplainSource(join.source)
		if join.kind != "CROSS" {
			detail += " ON " + sqlExplainExpression(join.on)
		}
		*steps = append(*steps, SQLExplainStep{Node: prefix + "JOIN", Detail: detail})
		if join.source.kind == "SUBQUERY" && join.source.query != nil {
			sqlAppendExplainSteps(steps, join.source.query, prefix+"  ")
		}
	}
	if query.where.kind != "" {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "FILTER", Detail: sqlExplainExpression(query.where)})
	}
	if len(query.groupBy) > 0 || sqlQueryHasAggregate(query) {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "AGGREGATE", Detail: sqlExplainExpressions(query.groupBy)})
	}
	if query.having.kind != "" {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "HAVING", Detail: sqlExplainExpression(query.having)})
	}
	*steps = append(*steps, SQLExplainStep{Node: prefix + "PROJECT", Detail: sqlExplainSelects(query.selects)})
	if query.distinct {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "DISTINCT", Detail: "deduplicate projected rows"})
	}
	if len(query.orderBy) > 0 {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "SORT", Detail: sqlExplainOrders(query.orderBy)})
	}
	if query.limit >= 0 || query.offset > 0 {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "LIMIT", Detail: fmt.Sprintf("limit=%d offset=%d", query.limit, query.offset)})
	}
	for _, union := range query.unions {
		kind := union.kind
		if union.all {
			kind += " ALL"
		}
		*steps = append(*steps, SQLExplainStep{Node: prefix + "SET", Detail: kind})
		sqlAppendExplainSteps(steps, union.query, prefix+"  ")
	}
}

func sqlExplainSourceStep(node string, source sqlSource) SQLExplainStep {
	step := SQLExplainStep{Node: node, Detail: sqlExplainSource(source)}
	if source.kind == "VALUES" {
		estimate := len(source.values)
		step.EstimatedRows = &estimate
	}
	return step
}

func sqlExplainSource(source sqlSource) string {
	var detail string
	switch source.kind {
	case "CACHE":
		detail = "CACHE(" + strconv.Quote(source.key) + ")"
	case "VALUES":
		detail = "VALUES"
	case "CTE":
		detail = "CTE " + source.key
	case "KEYS":
		detail = "KEYS"
	case "SUBQUERY":
		detail = "derived query"
	default:
		detail = source.kind
	}
	if source.alias != "" {
		detail += " AS " + source.alias
	}
	return detail
}

func sqlExplainExpression(expression sqlExpr) string {
	switch expression.kind {
	case "field":
		if expression.qualifier != "" {
			return expression.qualifier + "." + expression.name
		}
		return expression.name
	case "literal":
		return fmt.Sprintf("%#v", expression.value)
	case "star":
		return "*"
	case "func":
		return expression.name + "(" + sqlExplainExpressions(expression.args) + ")"
	case "unary":
		return expression.op + " " + sqlExplainExpression(*expression.left)
	case "binary":
		if expression.op == "IS NULL" || expression.op == "IS NOT NULL" {
			return sqlExplainExpression(*expression.left) + " " + expression.op
		}
		return sqlExplainExpression(*expression.left) + " " + expression.op + " " + sqlExplainExpression(*expression.right)
	}
	return "<unknown expression>"
}

func sqlExplainExpressions(expressions []sqlExpr) string {
	values := make([]string, len(expressions))
	for index, expression := range expressions {
		values[index] = sqlExplainExpression(expression)
	}
	return strings.Join(values, ", ")
}

func sqlExplainSelects(items []sqlSelectItem) string {
	values := make([]string, len(items))
	for index, item := range items {
		values[index] = sqlExplainExpression(item.expr)
		if item.alias != "" {
			values[index] += " AS " + item.alias
		}
	}
	return strings.Join(values, ", ")
}

func sqlExplainOrders(orders []sqlOrder) string {
	values := make([]string, len(orders))
	for index, order := range orders {
		values[index] = sqlExplainExpression(order.expr)
		if order.desc {
			values[index] += " DESC"
		} else {
			values[index] += " ASC"
		}
	}
	return strings.Join(values, ", ")
}

func sameSQLColumns(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !strings.EqualFold(left[index], right[index]) {
			return false
		}
	}
	return true
}

func distinctSQLQueryRows(rows []SQLRow) []SQLRow {
	seen := make(map[string]struct{}, len(rows))
	out := rows[:0]
	for _, row := range rows {
		key := sqlOutputRowKey(row)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, row)
	}
	return out
}

func sqlOutputRowKey(row SQLRow) string {
	if encoded, err := json.Marshal(row); err == nil {
		return string(encoded)
	}
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&builder, "%q=%#v;", key, row[key])
	}
	return builder.String()
}
func resolveSQLSource(source sqlSource, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics) ([]SQLRow, error) {
	switch source.kind {
	case "VALUES":
		return valuesSQLRows(source.values, source.columns), nil
	case "CTE":
		return ctes[source.key], nil
	case "SUBQUERY":
		result, err := executeSQLQueryWithMetrics(source.query, resolver, ctes, metrics)
		if err != nil {
			return nil, err
		}
		return result.Rows, nil
	case "CACHE", "KEYS":
		if resolver == nil {
			return nil, nil
		}
		return resolver.ResolveSQLSource(source.kind, source.key)
	}
	return nil, nil
}
func valuesSQLRows(values [][]interface{}, columns []string) []SQLRow {
	if len(columns) == 0 && len(values) > 0 {
		columns = make([]string, len(values[0]))
		for i := range columns {
			columns[i] = "column" + strconv.Itoa(i+1)
		}
	}
	out := make([]SQLRow, 0, len(values))
	for _, source := range values {
		row := SQLRow{}
		for i, value := range source {
			if i < len(columns) {
				row[columns[i]] = value
			}
		}
		out = append(out, row)
	}
	return out
}
func wrapSQLSource(source sqlSource, rows []SQLRow) []sqlExecRow {
	out := make([]sqlExecRow, len(rows))
	for i, row := range rows {
		out[i] = sqlExecRow{sources: map[string]SQLRow{source.alias: row}, order: []string{source.alias}}
	}
	return out
}
func mergeSQLRows(left, right sqlExecRow) sqlExecRow {
	out := sqlExecRow{sources: map[string]SQLRow{}, order: append(append([]string{}, left.order...), right.order...)}
	for k, v := range left.sources {
		out.sources[k] = v
	}
	for k, v := range right.sources {
		out.sources[k] = v
	}
	return out
}
func groupSQLRows(rows []sqlExecRow, by []sqlExpr, q *sqlQuery) [][]sqlExecRow {
	if len(by) == 0 {
		if !sqlQueryHasAggregate(q) {
			out := make([][]sqlExecRow, len(rows))
			for i, row := range rows {
				out[i] = []sqlExecRow{row}
			}
			return out
		}
		if len(rows) == 0 {
			return [][]sqlExecRow{{}}
		}
		return [][]sqlExecRow{rows}
	}
	groups := map[string][]sqlExecRow{}
	order := []string{}
	for _, row := range rows {
		parts := make([]string, len(by))
		for i, expr := range by {
			parts[i] = fmt.Sprintf("%#v", evalSQLExpr(expr, []sqlExecRow{row}, row))
		}
		key := strings.Join(parts, "\x00")
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], row)
	}
	out := make([][]sqlExecRow, 0, len(order))
	for _, key := range order {
		out = append(out, groups[key])
	}
	return out
}
func sqlQueryHasAggregate(q *sqlQuery) bool {
	for _, item := range q.selects {
		if sqlExprHasAggregate(item.expr) {
			return true
		}
	}
	return sqlExprHasAggregate(q.having)
}
func sqlExprHasAggregate(expr sqlExpr) bool {
	if expr.kind == "func" {
		switch expr.name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return true
		}
		for _, arg := range expr.args {
			if sqlExprHasAggregate(arg) {
				return true
			}
		}
	}
	if expr.left != nil && sqlExprHasAggregate(*expr.left) {
		return true
	}
	return expr.right != nil && sqlExprHasAggregate(*expr.right)
}
func sqlColumns(items []sqlSelectItem) []string {
	out := make([]string, len(items))
	for i, item := range items {
		if item.alias != "" {
			out[i] = item.alias
		} else if item.expr.kind == "field" {
			out[i] = item.expr.name
		} else if item.expr.kind == "func" {
			out[i] = strings.ToLower(item.expr.name)
		} else {
			out[i] = "column" + strconv.Itoa(i+1)
		}
	}
	return out
}
func evalSQLExpr(expr sqlExpr, group []sqlExecRow, row sqlExecRow) interface{} {
	switch expr.kind {
	case "literal":
		return expr.value
	case "field":
		return sqlField(row, expr.qualifier, expr.name)
	case "func":
		switch expr.name {
		case "COUNT":
			if len(expr.args) == 0 || expr.args[0].kind == "star" {
				return int64(len(group))
			}
			var n int64
			for _, r := range group {
				if evalSQLExpr(expr.args[0], []sqlExecRow{r}, r) != nil {
					n++
				}
			}
			return n
		case "SUM", "AVG", "MIN", "MAX":
			var values []float64
			for _, r := range group {
				if n, ok := sqlNumber(evalSQLExpr(expr.args[0], []sqlExecRow{r}, r)); ok {
					values = append(values, n)
				}
			}
			if len(values) == 0 {
				return nil
			}
			result := values[0]
			for _, v := range values[1:] {
				if expr.name == "SUM" || expr.name == "AVG" {
					result += v
				} else if expr.name == "MIN" && v < result {
					result = v
				} else if expr.name == "MAX" && v > result {
					result = v
				}
			}
			if expr.name == "AVG" {
				result /= float64(len(values))
			}
			return result
		}
	case "unary":
		value := evalSQLExpr(*expr.left, group, row)
		switch expr.op {
		case "!":
			return !sqlTruthy(value)
		case "-":
			switch number := value.(type) {
			case int64:
				return -number
			case int:
				return -number
			case float64:
				return -number
			case float32:
				return -number
			}
			return nil
		}
	case "binary":
		left := evalSQLExpr(*expr.left, group, row)
		if expr.op == "IS NULL" {
			return left == nil
		}
		if expr.op == "IS NOT NULL" {
			return left != nil
		}
		right := evalSQLExpr(*expr.right, group, row)
		return sqlBinaryValue(expr.op, left, right)
	}
	return nil
}
func sqlField(row sqlExecRow, qualifier, name string) interface{} {
	if qualifier != "" {
		return row.sources[qualifier][name]
	}
	for _, source := range row.order {
		if value, ok := row.sources[source][name]; ok {
			return value
		}
	}
	return nil
}
func evalOutputOrder(expr sqlExpr, out SQLRow, group []sqlExecRow) interface{} {
	if expr.kind == "field" && expr.qualifier == "" {
		if value, ok := out[expr.name]; ok {
			return value
		}
	}
	row := sqlExecRow{}
	if len(group) > 0 {
		row = group[0]
	}
	return evalSQLExpr(expr, group, row)
}
func sqlTruthy(value interface{}) bool {
	b, ok := value.(bool)
	if ok {
		return b
	}
	return value != nil && value != false
}
func sqlNumber(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	}
	return 0, false
}
func sqlCompare(left, right interface{}) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	if a, ok := sqlNumber(left); ok {
		if b, ok := sqlNumber(right); ok {
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		}
	}
	a, b := fmt.Sprint(left), fmt.Sprint(right)
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func sqlLike(value, pattern string) bool {
	parts := strings.Split(pattern, "%")
	if len(parts) == 1 {
		return value == pattern
	}
	if !strings.HasPrefix(pattern, "%") && !strings.HasPrefix(value, parts[0]) {
		return false
	}
	if !strings.HasSuffix(pattern, "%") && !strings.HasSuffix(value, parts[len(parts)-1]) {
		return false
	}
	position := 0
	for _, part := range parts {
		if part == "" {
			continue
		}
		index := strings.Index(value[position:], part)
		if index < 0 {
			return false
		}
		position += index + len(part)
	}
	return true
}

func sqlExprHasCustomFunction(expr sqlExpr, functions SQLFunctionResolver) bool {
	_ = functions
	if expr.kind == "func" && !sqlBuiltinFunction(expr.name) {
		return true
	}
	if expr.left != nil && sqlExprHasCustomFunction(*expr.left, functions) {
		return true
	}
	if expr.right != nil && sqlExprHasCustomFunction(*expr.right, functions) {
		return true
	}
	for _, argument := range expr.args {
		if sqlExprHasCustomFunction(argument, functions) {
			return true
		}
	}
	return false
}
func sqlBuiltinFunction(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	}
	return false
}
func evalSQLExprBatch(expr sqlExpr, rows []sqlExecRow, functions SQLFunctionResolver) ([]interface{}, error) {
	if expr.kind == "func" && !sqlBuiltinFunction(expr.name) {
		if functions == nil {
			return nil, fmt.Errorf("unknown SQL function %q", expr.name)
		}
		calls := make([]SQLFunctionCall, len(rows))
		for index, row := range rows {
			call := SQLFunctionCall{Arguments: make([]interface{}, len(expr.args))}
			for argIndex, arg := range expr.args {
				value, err := evalSQLExprBatch(arg, []sqlExecRow{row}, functions)
				if err != nil {
					return nil, err
				}
				call.Arguments[argIndex] = value[0]
			}
			calls[index] = call
		}
		values, err := functions.EvaluateSQLFunction(expr.name, calls)
		if err != nil {
			return nil, err
		}
		if len(values) != len(rows) {
			return nil, fmt.Errorf("SQL function %q returned %d values for %d rows", expr.name, len(values), len(rows))
		}
		return values, nil
	}
	if expr.kind == "binary" {
		left, err := evalSQLExprBatch(*expr.left, rows, functions)
		if err != nil {
			return nil, err
		}
		if expr.op == "IS NULL" || expr.op == "IS NOT NULL" {
			out := make([]interface{}, len(rows))
			for i := range rows {
				if expr.op == "IS NULL" {
					out[i] = left[i] == nil
				} else {
					out[i] = left[i] != nil
				}
			}
			return out, nil
		}
		right, err := evalSQLExprBatch(*expr.right, rows, functions)
		if err != nil {
			return nil, err
		}
		out := make([]interface{}, len(rows))
		for i := range rows {
			out[i] = sqlBinaryValue(expr.op, left[i], right[i])
		}
		return out, nil
	}
	out := make([]interface{}, len(rows))
	for index, row := range rows {
		out[index] = evalSQLExpr(expr, []sqlExecRow{row}, row)
	}
	return out, nil
}
func sqlBinaryValue(op string, left, right interface{}) interface{} {
	switch op {
	case "AND":
		return sqlTruthy(left) && sqlTruthy(right)
	case "OR":
		return sqlTruthy(left) || sqlTruthy(right)
	case "LIKE":
		return sqlLike(fmt.Sprint(left), fmt.Sprint(right))
	case "=":
		return sqlCompare(left, right) == 0
	case "!=", "<>":
		return sqlCompare(left, right) != 0
	case "<":
		return sqlCompare(left, right) < 0
	case "<=":
		return sqlCompare(left, right) <= 0
	case ">":
		return sqlCompare(left, right) > 0
	case ">=":
		return sqlCompare(left, right) >= 0
	case "+", "-", "*", "/", "%":
		return sqlArithmeticValue(op, left, right)
	}
	return nil
}

func sqlArithmeticValue(op string, left, right interface{}) interface{} {
	leftInteger, leftIsInteger := sqlInteger(left)
	rightInteger, rightIsInteger := sqlInteger(right)
	if leftIsInteger && rightIsInteger {
		switch op {
		case "+":
			return leftInteger + rightInteger
		case "-":
			return leftInteger - rightInteger
		case "*":
			return leftInteger * rightInteger
		case "/":
			if rightInteger == 0 {
				return nil
			}
			return leftInteger / rightInteger
		case "%":
			if rightInteger == 0 {
				return nil
			}
			return leftInteger % rightInteger
		}
	}
	leftNumber, leftOK := sqlNumber(left)
	rightNumber, rightOK := sqlNumber(right)
	if !leftOK || !rightOK || (op == "/" && rightNumber == 0) || op == "%" {
		return nil
	}
	switch op {
	case "+":
		return leftNumber + rightNumber
	case "-":
		return leftNumber - rightNumber
	case "*":
		return leftNumber * rightNumber
	case "/":
		return leftNumber / rightNumber
	}
	return nil
}

func sqlInteger(value interface{}) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int64:
		return number, true
	}
	return 0, false
}
