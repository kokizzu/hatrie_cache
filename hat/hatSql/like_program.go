package hatSql

import (
	"fmt"
	"strings"
)

// sqlLikeProgram retains the percent-separated parts of a literal LIKE
// pattern so the scalar evaluator does not split the same pattern per row.
type sqlLikeProgram struct {
	pattern string
	parts   []string
}

func prepareSQLLikeExpr(expr *sqlExpr) {
	if expr == nil {
		return
	}
	expr.likeProgram = nil
	if expr.kind != "binary" || expr.op != "LIKE" || expr.right == nil || expr.right.kind != "literal" {
		return
	}
	pattern, ok := expr.right.value.(string)
	if !ok {
		return
	}
	expr.likeProgram = &sqlLikeProgram{pattern: pattern, parts: strings.Split(pattern, "%")}
}

func (program *sqlLikeProgram) evaluate(left interface{}, collation SQLCollation) interface{} {
	if left == nil {
		return nil
	}
	if collation.normalized() != SQLCollationBinary {
		return sqlBinaryValueWithCollation("LIKE", left, program.pattern, collation)
	}
	value, ok := left.(string)
	if !ok {
		value = fmt.Sprint(left)
	}
	return sqlLikeParts(value, program.pattern, program.parts)
}

func sqlLikePredicateMatches(program *sqlLikeProgram, left interface{}, pattern string, collation SQLCollation) bool {
	if left == nil {
		return false
	}
	if program != nil {
		return program.evaluate(left, collation) == true
	}
	return sqlLike(fmt.Sprint(left), pattern)
}
