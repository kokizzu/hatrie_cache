package hatCache

import "hatrie_cache/hat/hatSql"

func newSQLGoFunction(definition SQLFunctionDefinition) (sqlFunctionRuntime, error) {
	return hatSql.NewGoFunction(definition)
}
