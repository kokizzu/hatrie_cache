package hatSql_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatRuntime"
	"hatrie_cache/hat/hatSql"
)

type tu04RuntimeQueryResolver struct {
	*hatSql.RuntimeFunctionResolver
	rows []hatSql.Row
}

func (resolver *tu04RuntimeQueryResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func TestTU04RuntimeFunctionResolver(t *testing.T) {
	addTwo, err := hatRuntime.Compile(1, []hatRuntime.Instruction{
		{Op: hatRuntime.OpLoadArg, Operand: 0},
		{Op: hatRuntime.OpPushInt, Operand: 2},
		{Op: hatRuntime.OpAddInt},
		{Op: hatRuntime.OpReturn},
	})
	if err != nil {
		t.Fatal(err)
	}
	appendBang, err := hatRuntime.Compile(1, []hatRuntime.Instruction{
		{Op: hatRuntime.OpLoadArg, Operand: 0},
		{Op: hatRuntime.OpPushString, Text: "!"},
		{Op: hatRuntime.OpConcat},
		{Op: hatRuntime.OpReturn},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := hatSql.NewRuntimeFunctionResolver(
		hatSql.RuntimeFunction{
			FunctionDefinition: hatSql.FunctionDefinition{
				Name:          "ADD_TWO",
				Arguments:     []string{"value"},
				Deterministic: true,
				Pure:          true,
			},
			Program: addTwo,
		},
		hatSql.RuntimeFunction{
			FunctionDefinition: hatSql.FunctionDefinition{Name: "APPEND_BANG"},
			Program:            appendBang,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	values, err := resolver.EvaluateSQLFunction("add_two", []hatSql.FunctionCall{
		{Arguments: []interface{}{int64(40)}},
		{Arguments: []interface{}{int(5)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []interface{}{int64(42), int64(7)}; len(values) != len(want) || values[0] != want[0] || values[1] != want[1] {
		t.Fatalf("ADD_TWO values = %#v, want %#v", values, want)
	}

	values, err = resolver.EvaluateSQLFunction("APPEND_BANG", []hatSql.FunctionCall{{Arguments: []interface{}{"row"}}})
	if err != nil || len(values) != 1 || values[0] != "row!" {
		t.Fatalf("APPEND_BANG values/error = %#v/%v, want [row!]/nil", values, err)
	}
	deterministic, pure, ok := resolver.FunctionCapabilities("add_two")
	if !ok || !deterministic || !pure {
		t.Fatalf("ADD_TWO capabilities = %t/%t/%t, want true/true/true", deterministic, pure, ok)
	}

	if _, err := resolver.EvaluateSQLFunction("missing", []hatSql.FunctionCall{{}}); !errors.Is(err, hatSql.ErrSQLFunctionNotHandled) {
		t.Fatalf("missing function error = %v, want ErrSQLFunctionNotHandled", err)
	}
	if _, err := resolver.EvaluateSQLFunction("ADD_TWO", []hatSql.FunctionCall{{Arguments: []interface{}{"wrong"}}}); !errors.Is(err, hatSql.ErrRuntimeFunctionArgument) {
		t.Fatalf("wrong type error = %v, want ErrRuntimeFunctionArgument", err)
	}
	if _, err := resolver.EvaluateSQLFunction("ADD_TWO", []hatSql.FunctionCall{{}}); !errors.Is(err, hatSql.ErrRuntimeFunctionArgument) {
		t.Fatalf("wrong arity error = %v, want ErrRuntimeFunctionArgument", err)
	}
}

func TestTU04RuntimeFunctionResolverRunsThroughSQL(t *testing.T) {
	program, err := hatRuntime.Compile(1, []hatRuntime.Instruction{
		{Op: hatRuntime.OpLoadArg, Operand: 0},
		{Op: hatRuntime.OpPushInt, Operand: 2},
		{Op: hatRuntime.OpAddInt},
		{Op: hatRuntime.OpReturn},
	})
	if err != nil {
		t.Fatal(err)
	}
	functions, err := hatSql.NewRuntimeFunctionResolver(hatSql.RuntimeFunction{
		FunctionDefinition: hatSql.FunctionDefinition{Name: "ADD_TWO"},
		Program:            program,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &tu04RuntimeQueryResolver{
		RuntimeFunctionResolver: functions,
		rows:                    []hatSql.Row{{"value": int64(40)}},
	}
	result, err := hatSql.ExecuteSQLQuery("FROM CACHE('items') AS items SELECT ADD_TWO(items.value) AS value", resolver)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["value"] != int64(42) {
		t.Fatalf("SQL runtime rows = %#v, want [{value:42}]", result.Rows)
	}
}
