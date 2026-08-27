package hatSql

import "fmt"

const maxSQLCubeDimensions = 12

func (p *sqlQueryParser) parseSQLGroupingClause() ([]sqlExpr, [][]sqlExpr, []sqlExpr, error) {
	if p.keyword("ROLLUP") {
		p.next()
		values, err := p.parseSQLGroupingSet(false)
		if err != nil {
			return nil, nil, nil, err
		}
		sets := make([][]sqlExpr, len(values)+1)
		for count := len(values); count >= 0; count-- {
			sets[len(values)-count] = cloneSQLExprs(values[:count])
		}
		return values, sets, cloneSQLExprs(values), nil
	}
	if p.keyword("CUBE") {
		p.next()
		values, err := p.parseSQLGroupingSet(false)
		if err != nil {
			return nil, nil, nil, err
		}
		if len(values) > maxSQLCubeDimensions {
			return nil, nil, nil, p.diagnostic(p.current(), fmt.Sprintf("CUBE supports at most %d dimensions", maxSQLCubeDimensions))
		}
		sets := make([][]sqlExpr, 0, 1<<len(values))
		for mask := (1 << len(values)) - 1; mask >= 0; mask-- {
			set := make([]sqlExpr, 0, len(values))
			for index := range values {
				if mask&(1<<index) != 0 {
					set = append(set, cloneSQLExpr(values[index]))
				}
			}
			sets = append(sets, set)
		}
		return values, sets, cloneSQLExprs(values), nil
	}
	if p.keyword("GROUPING") {
		p.next()
		if err := p.expectKeyword("SETS"); err != nil {
			return nil, nil, nil, err
		}
		if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
			return nil, nil, nil, err
		}
		sets := make([][]sqlExpr, 0, 1)
		dimensions := make([]sqlExpr, 0)
		for {
			set, err := p.parseSQLGroupingSet(true)
			if err != nil {
				return nil, nil, nil, err
			}
			sets = append(sets, set)
			for _, expr := range set {
				if !sqlGroupingSetContains(dimensions, expr) {
					dimensions = append(dimensions, cloneSQLExpr(expr))
				}
			}
			if p.current().kind != sqlTokenComma {
				break
			}
			p.next()
		}
		if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
			return nil, nil, nil, err
		}
		return cloneSQLExprs(dimensions), sets, dimensions, nil
	}
	values, err := p.parseExprList()
	return values, nil, nil, err
}

func (p *sqlQueryParser) parseSQLGroupingSet(allowEmpty bool) ([]sqlExpr, error) {
	if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
		return nil, err
	}
	if p.current().kind == sqlTokenRightParen {
		p.next()
		if !allowEmpty {
			return nil, p.diagnostic(p.current(), "grouping set cannot be empty")
		}
		return nil, nil
	}
	values, err := p.parseExprList()
	if err != nil {
		return nil, err
	}
	if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
		return nil, err
	}
	return values, nil
}

func sqlExpandGroupingSets(query *sqlQuery) error {
	if len(query.groupingSets) == 0 {
		return nil
	}
	if len(query.unions) != 0 {
		return fmt.Errorf("GROUPING SETS, ROLLUP, and CUBE cannot be combined with set operations")
	}
	template := cloneSQLQuery(query)
	for index, groupingSet := range query.groupingSets {
		branch := query
		if index != 0 {
			branch = cloneSQLQuery(template)
		}
		branch.groupBy = cloneSQLExprs(groupingSet)
		branch.groupingSets = nil
		branch.groupingDimensions = nil
		sqlNullAbsentGroupingDimensions(branch, groupingSet, template.groupingDimensions)
		if index != 0 {
			query.unions = append(query.unions, sqlUnion{kind: "UNION", all: true, query: branch})
		}
	}
	return nil
}

func sqlNullAbsentGroupingDimensions(query *sqlQuery, groupingSet, dimensions []sqlExpr) {
	for index := range query.selects {
		for _, dimension := range dimensions {
			if sqlSameGroupingExpr(query.selects[index].expr, dimension) && !sqlGroupingSetContains(groupingSet, dimension) {
				if query.selects[index].alias == "" {
					query.selects[index].alias = sqlGroupingDimensionColumn(query.selects[index].expr)
				}
				query.selects[index].expr = sqlExpr{kind: "literal", value: nil}
				break
			}
		}
	}
}

func sqlGroupingDimensionColumn(expr sqlExpr) string {
	if expr.kind == "field" && expr.name != "" {
		return expr.name
	}
	return sqlExplainExpression(expr)
}

func sqlGroupingSetContains(expressions []sqlExpr, target sqlExpr) bool {
	for _, expr := range expressions {
		if sqlSameGroupingExpr(expr, target) {
			return true
		}
	}
	return false
}

func sqlSameGroupingExpr(left, right sqlExpr) bool {
	if sqlSameField(left, right) {
		return true
	}
	return sqlExplainExpression(left) == sqlExplainExpression(right)
}

func cloneSQLGroupingSets(source [][]sqlExpr) [][]sqlExpr {
	if source == nil {
		return nil
	}
	sets := make([][]sqlExpr, len(source))
	for index, groupingSet := range source {
		sets[index] = cloneSQLExprs(groupingSet)
	}
	return sets
}
