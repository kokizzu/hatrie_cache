package hatSql

import (
	"context"
	"testing"
)

const tu03ProcedureBenchmarkSource = "FROM VALUES (1, 'Ada'), (2, 'Lin'), (3, 'Mika') AS users(id, name) WHERE users.id = $1 SELECT users.name"

func BenchmarkTU03SQLProcedureRegistry(b *testing.B) {
	registry, err := NewSQLProcedureRegistry(SQLProcedureRegistryOptions{MaxProcedures: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(SQLProcedureDefinition{Name: "user_by_id", Source: tu03ProcedureBenchmarkSource, Parameters: []string{"id"}}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	parameters := []interface{}{int64(2)}
	b.Run("compile-each-call", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			compiled, err := CompileSQLQuery(tu03ProcedureBenchmarkSource)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := compiled.Execute(ctx, nil, parameters, SQLQueryOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("registry-call", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, err := registry.Call(ctx, "USER_BY_ID", nil, parameters, SQLQueryOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
