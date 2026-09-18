package hatSql

import (
	"errors"
	"math"
	"strings"
)

// SQLMonotonicity describes how an expression changes as its selected input
// column increases. The proof is conservative: unsupported expressions are
// reported as Unknown.
type SQLMonotonicity uint8

const (
	SQLMonotonicityUnknown SQLMonotonicity = iota
	SQLMonotonicityConstant
	SQLMonotonicityNonDecreasing
	SQLMonotonicityNonIncreasing
)

func (monotonicity SQLMonotonicity) String() string {
	switch monotonicity {
	case SQLMonotonicityConstant:
		return "constant"
	case SQLMonotonicityNonDecreasing:
		return "non-decreasing"
	case SQLMonotonicityNonIncreasing:
		return "non-increasing"
	default:
		return "unknown"
	}
}

var (
	ErrSQLMonotonicityEmptyExpression = errors.New("SQL monotonicity expression is empty")
	ErrSQLMonotonicityEmptyColumn     = errors.New("SQL monotonicity column is empty")
)

// SQLMonotonicityReport is a proof result for one SQL expression relative to
// one input column. MayReturnNull is reported separately because SQL NULL
// ordering is a caller concern; the direction describes the non-NULL value
// domain and preserves NULL-producing operations for a caller-selected NULL
// placement.
type SQLMonotonicityReport struct {
	Expression      string
	Column          string
	Monotonicity    SQLMonotonicity
	DependsOnColumn bool
	MayReturnNull   bool
	Reason          string
}

// AnalyzeSQLExpressionMonotonicity parses and conservatively classifies one
// SQL expression relative to column. Qualified expressions such as
// orders.price match a qualified column exactly; an unqualified column name
// matches fields with that name regardless of their qualifier.
func AnalyzeSQLExpressionMonotonicity(expression, column string) (SQLMonotonicityReport, error) {
	expression = strings.TrimSpace(expression)
	column = strings.TrimSpace(column)
	if expression == "" {
		return SQLMonotonicityReport{}, ErrSQLMonotonicityEmptyExpression
	}
	if column == "" {
		return SQLMonotonicityReport{}, ErrSQLMonotonicityEmptyColumn
	}
	tokens, err := lexSQL(expression)
	if err != nil {
		return SQLMonotonicityReport{}, err
	}
	parser := sqlQueryParser{tokens: tokens}
	tree, err := parser.parseCondition()
	if err != nil {
		return SQLMonotonicityReport{}, err
	}
	if parser.current().kind == sqlTokenSemicolon {
		parser.next()
	}
	if parser.current().kind != sqlTokenEOF {
		return SQLMonotonicityReport{}, parser.expected(parser.current(), "end of input", nil)
	}
	analysis := analyzeSQLMonotonicityExpr(tree, column)
	return SQLMonotonicityReport{
		Expression:      expression,
		Column:          column,
		Monotonicity:    analysis.direction,
		DependsOnColumn: analysis.depends,
		MayReturnNull:   analysis.mayReturnNull,
		Reason:          analysis.reason,
	}, nil
}

type sqlMonotonicityAnalysis struct {
	direction     SQLMonotonicity
	depends       bool
	mayReturnNull bool
	reason        string
}

func sqlMonotonicityResult(direction SQLMonotonicity, depends, mayReturnNull bool, reason string) sqlMonotonicityAnalysis {
	if reason == "" {
		reason = direction.String()
	}
	return sqlMonotonicityAnalysis{
		direction:     direction,
		depends:       depends,
		mayReturnNull: mayReturnNull,
		reason:        reason,
	}
}

func sqlMonotonicityUnknown(depends, mayReturnNull bool, reason string) sqlMonotonicityAnalysis {
	return sqlMonotonicityResult(SQLMonotonicityUnknown, depends, mayReturnNull, reason)
}

func analyzeSQLMonotonicityExpr(expr sqlExpr, column string) sqlMonotonicityAnalysis {
	switch expr.kind {
	case "literal":
		return sqlMonotonicityResult(SQLMonotonicityConstant, false, expr.value == nil, "literal is constant")
	case "field":
		if sqlMonotonicityFieldMatches(expr, column) {
			return sqlMonotonicityResult(SQLMonotonicityNonDecreasing, true, true, "field is the selected input column")
		}
		return sqlMonotonicityUnknown(false, true, "field is independent of the selected input column")
	case "unary":
		if expr.left == nil {
			return sqlMonotonicityUnknown(false, true, "unary expression has no operand")
		}
		operand := analyzeSQLMonotonicityExpr(*expr.left, column)
		switch expr.op {
		case "-", "!":
			return sqlMonotonicityFlip(operand, "unary "+expr.op)
		default:
			return sqlMonotonicityUnknown(operand.depends, true, "unsupported unary operator "+expr.op)
		}
	case "binary":
		if expr.left == nil {
			return sqlMonotonicityUnknown(false, true, "binary expression has no left operand")
		}
		left := analyzeSQLMonotonicityExpr(*expr.left, column)
		if expr.right == nil {
			if expr.op == "IS NULL" || expr.op == "IS NOT NULL" {
				if left.direction == SQLMonotonicityConstant {
					return sqlMonotonicityResult(SQLMonotonicityConstant, false, false, "NULL test of a constant")
				}
				return sqlMonotonicityUnknown(left.depends, false, "NULL test depends on input NULL state")
			}
			return sqlMonotonicityUnknown(left.depends, true, "binary expression has no right operand")
		}
		right := analyzeSQLMonotonicityExpr(*expr.right, column)
		switch expr.op {
		case "+", "-", "*", "/", "%":
			return sqlMonotonicityArithmetic(expr.op, left, right, *expr.left, *expr.right)
		case "AND", "OR":
			return sqlMonotonicityBoolean(expr.op, left, right, *expr.left, *expr.right)
		case "=", "!=", "<>", "<", "<=", ">", ">=":
			return sqlMonotonicityComparison(expr.op, left, right, *expr.left, *expr.right)
		case "IS NULL", "IS NOT NULL", "LIKE", "REGEXP", "NOT REGEXP":
			return sqlMonotonicityUnknown(left.depends || right.depends, true, "comparison is not proven monotonic")
		default:
			return sqlMonotonicityUnknown(left.depends || right.depends, true, "unsupported binary operator "+expr.op)
		}
	case "in", "between":
		analysis := analyzeSQLMonotonicityExprList(expr, column)
		if !analysis.depends && analysis.direction == SQLMonotonicityConstant {
			return sqlMonotonicityResult(SQLMonotonicityConstant, false, analysis.mayReturnNull, "membership expression is constant")
		}
		return sqlMonotonicityUnknown(analysis.depends, true, "membership expression is not proven monotonic")
	case "case":
		return sqlMonotonicityCase(expr, column)
	case "func":
		analysis := analyzeSQLMonotonicityExprList(expr, column)
		return sqlMonotonicityUnknown(analysis.depends, true, "function has no monotonicity proof")
	case "subquery":
		return sqlMonotonicityUnknown(true, true, "subquery has no monotonicity proof")
	default:
		return sqlMonotonicityUnknown(true, true, "unsupported expression kind "+expr.kind)
	}
}

func sqlMonotonicityFieldMatches(expr sqlExpr, column string) bool {
	if strings.Contains(column, ".") {
		return expr.qualifier != "" && strings.EqualFold(expr.qualifier+"."+expr.name, column)
	}
	return strings.EqualFold(expr.name, column)
}

func sqlMonotonicityFlip(value sqlMonotonicityAnalysis, operation string) sqlMonotonicityAnalysis {
	direction := value.direction
	switch direction {
	case SQLMonotonicityNonDecreasing:
		direction = SQLMonotonicityNonIncreasing
	case SQLMonotonicityNonIncreasing:
		direction = SQLMonotonicityNonDecreasing
	}
	return sqlMonotonicityResult(direction, value.depends, value.mayReturnNull, operation+" preserves or flips the proven direction")
}

func sqlMonotonicityArithmetic(op string, left, right sqlMonotonicityAnalysis, leftExpr, rightExpr sqlExpr) sqlMonotonicityAnalysis {
	if sqlMonotonicityAlwaysNull(leftExpr) || sqlMonotonicityAlwaysNull(rightExpr) {
		return sqlMonotonicityResult(SQLMonotonicityConstant, false, true, "arithmetic with NULL is constant NULL")
	}
	mayReturnNull := left.mayReturnNull || right.mayReturnNull
	if left.direction == SQLMonotonicityConstant && right.direction == SQLMonotonicityConstant {
		if op == "/" {
			if denominator, ok := sqlMonotonicityConstantNumber(rightExpr); !ok || denominator == 0 {
				return sqlMonotonicityUnknown(left.depends || right.depends, true, "constant division is not proven safe")
			}
		}
		return sqlMonotonicityResult(SQLMonotonicityConstant, false, mayReturnNull, "arithmetic combines constants")
	}
	switch op {
	case "+":
		if left.direction == SQLMonotonicityConstant {
			return sqlMonotonicityResult(right.direction, right.depends, mayReturnNull, "adding a constant preserves direction")
		}
		if right.direction == SQLMonotonicityConstant {
			return sqlMonotonicityResult(left.direction, left.depends, mayReturnNull, "adding a constant preserves direction")
		}
	case "-":
		if right.direction == SQLMonotonicityConstant {
			return sqlMonotonicityResult(left.direction, left.depends, mayReturnNull, "subtracting a constant preserves direction")
		}
		if left.direction == SQLMonotonicityConstant {
			return sqlMonotonicityFlip(right, "constant minus expression")
		}
	case "*":
		if factor, ok := sqlMonotonicityConstantNumber(rightExpr); ok && right.direction == SQLMonotonicityConstant {
			return sqlMonotonicityScale(left, factor, mayReturnNull)
		}
		if factor, ok := sqlMonotonicityConstantNumber(leftExpr); ok && left.direction == SQLMonotonicityConstant {
			return sqlMonotonicityScale(right, factor, mayReturnNull)
		}
	case "/":
		if denominator, ok := sqlMonotonicityConstantNumber(rightExpr); ok && right.direction == SQLMonotonicityConstant {
			if denominator == 0 {
				return sqlMonotonicityUnknown(left.depends || right.depends, true, "division by zero is not proven safe")
			}
			return sqlMonotonicityScale(left, 1/denominator, mayReturnNull)
		}
	}
	return sqlMonotonicityUnknown(left.depends || right.depends, true, "arithmetic combines non-constant expressions")
}

func sqlMonotonicityScale(value sqlMonotonicityAnalysis, factor float64, mayReturnNull bool) sqlMonotonicityAnalysis {
	if math.IsNaN(factor) || math.IsInf(factor, 0) {
		return sqlMonotonicityUnknown(value.depends, true, "non-finite scale is not proven safe")
	}
	if factor == 0 || value.direction == SQLMonotonicityConstant {
		return sqlMonotonicityResult(SQLMonotonicityConstant, false, mayReturnNull, "zero or constant scale removes input dependence")
	}
	if factor < 0 {
		return sqlMonotonicityResult(sqlMonotonicityFlipDirection(value.direction), value.depends, mayReturnNull, "negative scale reverses direction")
	}
	return sqlMonotonicityResult(value.direction, value.depends, mayReturnNull, "positive scale preserves direction")
}

func sqlMonotonicityFlipDirection(direction SQLMonotonicity) SQLMonotonicity {
	switch direction {
	case SQLMonotonicityNonDecreasing:
		return SQLMonotonicityNonIncreasing
	case SQLMonotonicityNonIncreasing:
		return SQLMonotonicityNonDecreasing
	default:
		return direction
	}
}

func sqlMonotonicityComparison(op string, left, right sqlMonotonicityAnalysis, leftExpr, rightExpr sqlExpr) sqlMonotonicityAnalysis {
	if sqlMonotonicityAlwaysNull(leftExpr) || sqlMonotonicityAlwaysNull(rightExpr) {
		return sqlMonotonicityResult(SQLMonotonicityConstant, false, true, "comparison with NULL is constant NULL")
	}
	mayReturnNull := left.mayReturnNull || right.mayReturnNull
	if left.direction == SQLMonotonicityConstant && right.direction == SQLMonotonicityConstant {
		return sqlMonotonicityResult(SQLMonotonicityConstant, false, mayReturnNull, "comparison combines constants")
	}
	if op == "=" || op == "!=" || op == "<>" {
		return sqlMonotonicityUnknown(left.depends || right.depends, true, "equality is not globally monotonic")
	}
	if right.direction == SQLMonotonicityConstant && left.direction != SQLMonotonicityUnknown {
		return sqlMonotonicityResult(sqlMonotonicityComparisonDirection(left.direction, op), left.depends, mayReturnNull, "ordered expression compared with a constant")
	}
	if left.direction == SQLMonotonicityConstant && right.direction != SQLMonotonicityUnknown {
		reversed := sqlMonotonicityReverseComparison(op)
		return sqlMonotonicityResult(sqlMonotonicityComparisonDirection(right.direction, reversed), right.depends, mayReturnNull, "constant compared with an ordered expression")
	}
	return sqlMonotonicityUnknown(left.depends || right.depends, true, "ordered comparison lacks one constant side")
}

func sqlMonotonicityComparisonDirection(direction SQLMonotonicity, op string) SQLMonotonicity {
	if op == "<" || op == "<=" {
		return sqlMonotonicityFlipDirection(direction)
	}
	return direction
}

func sqlMonotonicityReverseComparison(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return op
	}
}

func sqlMonotonicityBoolean(op string, left, right sqlMonotonicityAnalysis, leftExpr, rightExpr sqlExpr) sqlMonotonicityAnalysis {
	if left.direction == SQLMonotonicityConstant && right.direction == SQLMonotonicityConstant {
		return sqlMonotonicityResult(SQLMonotonicityConstant, false, left.mayReturnNull || right.mayReturnNull, "boolean operation combines constants")
	}
	if value, ok := sqlMonotonicityBooleanLiteral(leftExpr); ok {
		if (op == "AND" && value == false) || (op == "OR" && value == true) {
			return sqlMonotonicityResult(SQLMonotonicityConstant, false, false, "boolean constant determines result")
		}
		return sqlMonotonicityResult(right.direction, right.depends, right.mayReturnNull, "neutral boolean constant preserves direction")
	}
	if value, ok := sqlMonotonicityBooleanLiteral(rightExpr); ok {
		if (op == "AND" && value == false) || (op == "OR" && value == true) {
			return sqlMonotonicityResult(SQLMonotonicityConstant, false, false, "boolean constant determines result")
		}
		return sqlMonotonicityResult(left.direction, left.depends, left.mayReturnNull, "neutral boolean constant preserves direction")
	}
	if left.direction == right.direction && (left.direction == SQLMonotonicityNonDecreasing || left.direction == SQLMonotonicityNonIncreasing) {
		return sqlMonotonicityResult(left.direction, left.depends || right.depends, left.mayReturnNull || right.mayReturnNull, "boolean operation combines equal directions")
	}
	return sqlMonotonicityUnknown(left.depends || right.depends, true, "boolean operation lacks compatible directions")
}

func sqlMonotonicityCase(expr sqlExpr, column string) sqlMonotonicityAnalysis {
	analysis := analyzeSQLMonotonicityExprList(expr, column)
	if !analysis.depends && analysis.direction == SQLMonotonicityConstant {
		return sqlMonotonicityResult(SQLMonotonicityConstant, false, analysis.mayReturnNull, "CASE is constant")
	}
	return sqlMonotonicityUnknown(analysis.depends, true, "CASE branch conditions are not proven monotonic")
}

func analyzeSQLMonotonicityExprList(expr sqlExpr, column string) sqlMonotonicityAnalysis {
	result := sqlMonotonicityResult(SQLMonotonicityConstant, false, false, "constant expression list")
	allConstant := true
	visit := func(value sqlMonotonicityAnalysis) {
		result.depends = result.depends || value.depends
		result.mayReturnNull = result.mayReturnNull || value.mayReturnNull
		if value.direction != SQLMonotonicityConstant {
			allConstant = false
		}
	}
	if expr.left != nil {
		visit(analyzeSQLMonotonicityExpr(*expr.left, column))
	}
	if expr.right != nil {
		visit(analyzeSQLMonotonicityExpr(*expr.right, column))
	}
	for _, argument := range expr.args {
		visit(analyzeSQLMonotonicityExpr(argument, column))
	}
	for _, branch := range expr.cases {
		visit(analyzeSQLMonotonicityExpr(branch.when, column))
		visit(analyzeSQLMonotonicityExpr(branch.then, column))
	}
	if expr.filter != nil {
		visit(analyzeSQLMonotonicityExpr(*expr.filter, column))
	}
	if expr.window != nil {
		for _, partition := range expr.window.partition {
			visit(analyzeSQLMonotonicityExpr(partition, column))
		}
		for _, order := range expr.window.order {
			visit(analyzeSQLMonotonicityExpr(order.expr, column))
		}
	}
	if !allConstant {
		result.direction = SQLMonotonicityUnknown
	}
	return result
}

func sqlMonotonicityConstantNumber(expr sqlExpr) (float64, bool) {
	if expr.kind == "literal" {
		return sqlNumber(expr.value)
	}
	if expr.kind == "unary" && expr.op == "-" && expr.left != nil {
		value, ok := sqlMonotonicityConstantNumber(*expr.left)
		return -value, ok
	}
	return 0, false
}

func sqlMonotonicityAlwaysNull(expr sqlExpr) bool {
	if expr.kind == "literal" {
		return expr.value == nil
	}
	if expr.kind == "unary" && expr.left != nil {
		return sqlMonotonicityAlwaysNull(*expr.left)
	}
	if expr.kind == "binary" && expr.left != nil && expr.right != nil {
		switch expr.op {
		case "+", "-", "*", "/", "%", "=", "!=", "<>", "<", "<=", ">", ">=", "LIKE", "REGEXP", "NOT REGEXP":
			return sqlMonotonicityAlwaysNull(*expr.left) || sqlMonotonicityAlwaysNull(*expr.right)
		}
	}
	return false
}

func sqlMonotonicityBooleanLiteral(expr sqlExpr) (bool, bool) {
	if expr.kind != "literal" {
		return false, false
	}
	value, ok := expr.value.(bool)
	return value, ok
}
