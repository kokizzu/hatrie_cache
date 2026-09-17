package hatSql

import "strings"

func sqlColumnarStringComparison(expr sqlExpr, alias string) (field, operator, value string, ok bool) {
	if expr.kind != "binary" || expr.left == nil || expr.right == nil || expr.collation.normalized() != SQLCollationBinary {
		return "", "", "", false
	}
	if !sqlColumnarStringComparisonOperator(expr.op) {
		return "", "", "", false
	}
	if expr.left.kind == "field" && (expr.left.qualifier == "" || expr.left.qualifier == alias) && expr.right.kind == "literal" {
		value, ok = expr.right.value.(string)
		return expr.left.name, expr.op, value, ok
	}
	if expr.right.kind == "field" && (expr.right.qualifier == "" || expr.right.qualifier == alias) && expr.left.kind == "literal" {
		value, ok = expr.left.value.(string)
		return expr.right.name, sqlInvertStringComparisonOperator(expr.op), value, ok
	}
	return "", "", "", false
}

func sqlColumnarStringComparisonOperator(operator string) bool {
	switch operator {
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func sqlInvertStringComparisonOperator(operator string) string {
	switch operator {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return operator
	}
}

func sqlColumnarPlainStringValues(batch ColumnarBatch, field string) ([]interface{}, bool) {
	if batch.fieldOffsets != nil || batch.decompressedBlockCache != nil {
		return nil, false
	}
	values, ok := batch.Columns[field]
	if !ok || len(values) != batch.Rows {
		return nil, false
	}
	if _, exists := batch.Dictionaries[field]; exists {
		return nil, false
	}
	if _, exists := batch.PackedColumns[field]; exists {
		return nil, false
	}
	if _, exists := batch.BoolColumns[field]; exists {
		return nil, false
	}
	if _, exists := batch.NumericColumns[field]; exists {
		return nil, false
	}
	if _, exists := batch.ListColumns[field]; exists {
		return nil, false
	}
	if _, exists := batch.NestedColumns[field]; exists {
		return nil, false
	}
	if _, exists := batch.MapColumns[field]; exists {
		return nil, false
	}
	for _, candidate := range values {
		if candidate == nil {
			continue
		}
		if _, ok := candidate.(string); !ok {
			return nil, false
		}
	}
	return values, true
}

func sqlColumnarStringComparisonMatches(candidate, operator, value string) bool {
	comparison := strings.Compare(candidate, value)
	switch operator {
	case "=":
		return comparison == 0
	case "!=", "<>":
		return comparison != 0
	case "<":
		return comparison < 0
	case "<=":
		return comparison <= 0
	case ">":
		return comparison > 0
	case ">=":
		return comparison >= 0
	default:
		return false
	}
}
