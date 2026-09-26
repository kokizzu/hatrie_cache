package hatSql

type sqlColumnarDictionaryCodeMask struct {
	bits uint64
	wide []bool
}

func (mask sqlColumnarDictionaryCodeMask) matches(code uint32) bool {
	if mask.wide != nil {
		return uint64(code) < uint64(len(mask.wide)) && mask.wide[code]
	}
	return code < 64 && mask.bits&(uint64(1)<<code) != 0
}

// sqlColumnarDictionaryOrderedPredicate recognizes binary-collation ordering
// against one dictionary string literal and precomputes the matching codes.
// The general evaluator remains authoritative for Unicode collations and wider
// expression shapes.
func sqlColumnarDictionaryOrderedPredicate(expr sqlExpr, alias string, batch ColumnarBatch) (DictionaryColumn, sqlColumnarDictionaryCodeMask, bool) {
	if expr.kind != "binary" || expr.left == nil || expr.right == nil || expr.collation.normalized() != SQLCollationBinary {
		return DictionaryColumn{}, sqlColumnarDictionaryCodeMask{}, false
	}
	field, operator := "", expr.op
	var literal interface{}
	if expr.left.kind == "field" && (expr.left.qualifier == "" || expr.left.qualifier == alias) && expr.right.kind == "literal" {
		field, literal = expr.left.name, expr.right.value
	} else if expr.right.kind == "field" && (expr.right.qualifier == "" || expr.right.qualifier == alias) && expr.left.kind == "literal" {
		field, literal = expr.right.name, expr.left.value
		operator = sqlReverseComparisonOperator(operator)
	}
	value, ok := literal.(string)
	if field == "" || !ok || !sqlColumnarDictionaryOrderingOperator(operator) {
		return DictionaryColumn{}, sqlColumnarDictionaryCodeMask{}, false
	}
	dictionary, ok := batch.Dictionaries[field]
	if !ok {
		return DictionaryColumn{}, sqlColumnarDictionaryCodeMask{}, false
	}
	mask := sqlColumnarDictionaryCodeMask{}
	if dictionary.ValueCount() > 64 {
		mask.wide = make([]bool, dictionary.ValueCount())
	}
	for code := 0; code < dictionary.ValueCount(); code++ {
		candidate, valid := dictionary.ValueAt(uint32(code))
		if !valid {
			return DictionaryColumn{}, sqlColumnarDictionaryCodeMask{}, false
		}
		if sqlColumnarStringComparisonMatches(candidate, operator, value) {
			if mask.wide != nil {
				mask.wide[code] = true
			} else {
				mask.bits |= uint64(1) << uint(code)
			}
		}
	}
	return dictionary, mask, true
}

func sqlColumnarDictionaryOrderingOperator(operator string) bool {
	switch operator {
	case "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

// sqlColumnarDictionaryBetweenPredicate recognizes a binary-collation range
// over one dictionary string field and precomputes the matching codes. Literal
// bounds keep NULL and dynamic-expression semantics on the general evaluator.
func sqlColumnarDictionaryBetweenPredicate(expr sqlExpr, alias string, batch ColumnarBatch) (DictionaryColumn, sqlColumnarDictionaryCodeMask, bool) {
	if expr.kind != "between" || expr.op != "BETWEEN" || expr.left == nil || expr.left.kind != "field" || (expr.left.qualifier != "" && expr.left.qualifier != alias) || len(expr.args) != 2 || expr.collation.normalized() != SQLCollationBinary {
		return DictionaryColumn{}, sqlColumnarDictionaryCodeMask{}, false
	}
	lower, lowerOK := expr.args[0].value.(string)
	upper, upperOK := expr.args[1].value.(string)
	if expr.args[0].kind != "literal" || expr.args[1].kind != "literal" || !lowerOK || !upperOK {
		return DictionaryColumn{}, sqlColumnarDictionaryCodeMask{}, false
	}
	dictionary, ok := batch.Dictionaries[expr.left.name]
	if !ok {
		return DictionaryColumn{}, sqlColumnarDictionaryCodeMask{}, false
	}
	mask := sqlColumnarDictionaryCodeMask{}
	if dictionary.ValueCount() > 64 {
		mask.wide = make([]bool, dictionary.ValueCount())
	}
	for code := 0; code < dictionary.ValueCount(); code++ {
		candidate, valid := dictionary.ValueAt(uint32(code))
		if !valid {
			return DictionaryColumn{}, sqlColumnarDictionaryCodeMask{}, false
		}
		matches := sqlColumnarStringComparisonMatches(candidate, ">=", lower) && sqlColumnarStringComparisonMatches(candidate, "<=", upper)
		if mask.wide != nil {
			mask.wide[code] = matches
		} else if matches {
			mask.bits |= uint64(1) << uint(code)
		}
	}
	return dictionary, mask, true
}
