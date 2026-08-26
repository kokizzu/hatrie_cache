//go:build !luajit

package hatCache

import "fmt"

func newSQLLuaFunction(definition SQLFunctionDefinition) (sqlFunctionRuntime, error) {
	return nil, fmt.Errorf("SQL function %q uses LANGUAGE LUA, but this binary was built without LuaJIT; rebuild with -tags luajit and the LuaJIT development library", definition.Name)
}
