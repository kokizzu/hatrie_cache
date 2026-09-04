package hatSql

import (
	"fmt"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// SQLCollation controls text comparison for one query. The default binary
// collation preserves the historical byte-wise behavior.
type SQLCollation string

const (
	SQLCollationBinary    SQLCollation = "BINARY"
	SQLCollationUnicodeCI SQLCollation = "UNICODE_CI"
)

func (collation SQLCollation) normalized() SQLCollation {
	if collation == "" {
		return SQLCollationBinary
	}
	return SQLCollation(strings.ToUpper(strings.TrimSpace(string(collation))))
}

func (collation SQLCollation) valid() bool {
	switch collation.normalized() {
	case SQLCollationBinary, SQLCollationUnicodeCI:
		return true
	default:
		return false
	}
}

func sqlCollationKey(collation SQLCollation, value string) string {
	if collation.normalized() == SQLCollationUnicodeCI {
		return norm.NFC.String(cases.Fold().String(value))
	}
	return value
}

func sqlCompareWithCollation(collation SQLCollation, left, right interface{}) int {
	if leftText, ok := left.(string); ok {
		if rightText, ok := right.(string); ok {
			return strings.Compare(sqlCollationKey(collation, leftText), sqlCollationKey(collation, rightText))
		}
	}
	return sqlCompare(left, right)
}

func sqlCollationValueKey(collation SQLCollation, value interface{}) string {
	if text, ok := value.(string); ok {
		return "text:" + sqlCollationKey(collation, text)
	}
	return fmt.Sprintf("%T:%#v", value, value)
}

func sqlLikeWithCollation(collation SQLCollation, value, pattern string) bool {
	return sqlLike(sqlCollationKey(collation, value), sqlCollationKey(collation, pattern))
}

func sqlBinaryValueWithCollation(op string, left, right interface{}, collation SQLCollation) interface{} {
	switch op {
	case "LIKE", "=", "!=", "<>", "<", "<=", ">", ">=":
		if left == nil || right == nil {
			return nil
		}
	}
	switch op {
	case "LIKE":
		return sqlLikeWithCollation(collation, fmt.Sprint(left), fmt.Sprint(right))
	case "=":
		return sqlCompareWithCollation(collation, left, right) == 0
	case "!=", "<>":
		return sqlCompareWithCollation(collation, left, right) != 0
	case "<":
		return sqlCompareWithCollation(collation, left, right) < 0
	case "<=":
		return sqlCompareWithCollation(collation, left, right) <= 0
	case ">":
		return sqlCompareWithCollation(collation, left, right) > 0
	case ">=":
		return sqlCompareWithCollation(collation, left, right) >= 0
	default:
		return sqlBinaryValue(op, left, right)
	}
}

func sqlInValueWithCollation(op string, left interface{}, values []interface{}, collation SQLCollation) interface{} {
	if left == nil {
		return nil
	}
	unknown := false
	for _, value := range values {
		comparison := sqlBinaryValueWithCollation("=", left, value, collation)
		if comparison == true {
			return op != "NOT IN"
		}
		if comparison == nil {
			unknown = true
		}
	}
	if unknown {
		return nil
	}
	return op == "NOT IN"
}

func sqlBetweenValueWithCollation(op string, left, lower, upper interface{}, collation SQLCollation) interface{} {
	value := sqlBinaryValue("AND", sqlBinaryValueWithCollation(">=", left, lower, collation), sqlBinaryValueWithCollation("<=", left, upper, collation))
	if value == nil || op != "NOT BETWEEN" {
		return value
	}
	return !sqlTruthy(value)
}

func applySQLQueryCollation(query *sqlQuery, collation SQLCollation) {
	if query == nil {
		return
	}
	collation = collation.normalized()
	for index := range query.ctes {
		applySQLQueryCollation(query.ctes[index].query, collation)
	}
	applySQLSourceCollation(query.from, collation)
	for index := range query.joins {
		applySQLSourceCollation(&query.joins[index].source, collation)
		applySQLExprCollation(&query.joins[index].on, collation)
	}
	for index := range query.selects {
		applySQLExprCollation(&query.selects[index].expr, collation)
	}
	applySQLExprCollation(&query.where, collation)
	for index := range query.groupBy {
		applySQLExprCollation(&query.groupBy[index], collation)
	}
	applySQLExprCollation(&query.having, collation)
	for index := range query.orderBy {
		applySQLOrderCollation(&query.orderBy[index], collation)
	}
	if query.limitBy != nil {
		for index := range query.limitBy.expressions {
			applySQLExprCollation(&query.limitBy.expressions[index], collation)
		}
	}
	for index := range query.unions {
		applySQLQueryCollation(query.unions[index].query, collation)
	}
}

func sqlQueryCollation(query *sqlQuery) SQLCollation {
	if query == nil {
		return SQLCollationBinary
	}
	for _, item := range query.selects {
		if item.expr.collation != "" {
			return item.expr.collation.normalized()
		}
	}
	return SQLCollationBinary
}

func applySQLSourceCollation(source *sqlSource, collation SQLCollation) {
	if source != nil {
		applySQLQueryCollation(source.query, collation)
	}
}

func applySQLOrderCollation(order *sqlOrder, collation SQLCollation) {
	order.collation = collation
	applySQLExprCollation(&order.expr, collation)
}

func applySQLExprCollation(expr *sqlExpr, collation SQLCollation) {
	if expr == nil {
		return
	}
	expr.collation = collation
	applySQLExprCollation(expr.left, collation)
	applySQLExprCollation(expr.right, collation)
	for index := range expr.args {
		applySQLExprCollation(&expr.args[index], collation)
	}
	for index := range expr.cases {
		applySQLExprCollation(&expr.cases[index].when, collation)
		applySQLExprCollation(&expr.cases[index].then, collation)
	}
	if expr.window != nil {
		for index := range expr.window.partition {
			applySQLExprCollation(&expr.window.partition[index], collation)
		}
		for index := range expr.window.order {
			applySQLOrderCollation(&expr.window.order[index], collation)
		}
	}
}
