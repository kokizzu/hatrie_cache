package hatSql

type sqlInProgram struct {
	values []interface{}
}

func prepareSQLInExpr(expr *sqlExpr) {
	if expr == nil {
		return
	}
	expr.inProgram = nil
	if expr.kind != "in" || len(expr.args) == 0 {
		return
	}
	values := make([]interface{}, len(expr.args))
	for index, argument := range expr.args {
		if argument.kind != "literal" {
			return
		}
		values[index] = argument.value
	}
	expr.inProgram = &sqlInProgram{values: values}
}
