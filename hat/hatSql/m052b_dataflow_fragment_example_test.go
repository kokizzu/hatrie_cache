package hatSql_test

import (
	"context"
	"fmt"

	hatSql "hatrie_cache/hat/hatSql"
)

func ExampleCompiledSQLQuery_CompileDataflow() {
	compiled, err := hatSql.CompileSQLQuery("FROM VALUES (1) AS src(id) SELECT src.id")
	if err != nil {
		panic(err)
	}
	executor, err := compiled.CompileDataflow(func(_ context.Context, _ hatSql.SQLDataflowFragment, inputs hatSql.SQLDataflowFragmentInputs) ([]hatSql.SQLRow, error) {
		if inputs.Len() == 0 {
			return inputs.Initial(), nil
		}
		return inputs.Rows(0), nil
	})
	if err != nil {
		panic(err)
	}
	rows, err := executor.Execute(context.Background(), []hatSql.SQLRow{{"id": int64(1)}})
	if err != nil {
		panic(err)
	}
	fmt.Println(rows[0]["id"])
	// Output: 1
}
