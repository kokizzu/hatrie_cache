package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTableFunctionResolverCanBeImplementedOutsidePackage(t *testing.T) {
	var resolver hatSql.TableFunctionResolver = testTableFunctionResolver{}
	result, err := hatSql.ExecuteSQLQuery("FROM TABLE(series(3, 2)) AS item SELECT item.value ORDER BY item.value", resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("len(result.Rows) = %d, want 3", len(result.Rows))
	}
	for index, want := range []int64{2, 3, 4} {
		if got := result.Rows[index]["value"]; got != want {
			t.Fatalf("result.Rows[%d][value] = %#v, want %d", index, got, want)
		}
	}
}

func TestTableFunctionRequiresResolver(t *testing.T) {
	_, err := hatSql.ExecuteSQLQuery("FROM TABLE(series(1)) SELECT value", nil)
	if err == nil || err.Error() != "TABLE(\"series\") requires a table function resolver" {
		t.Fatalf("ExecuteSQLQuery() error = %v, want missing table function resolver error", err)
	}
}

func TestTableFunctionBindsPreparedArguments(t *testing.T) {
	result, err := hatSql.ExecuteSQLQueryParameters(
		context.Background(),
		"FROM TABLE(series($1, $2)) AS item SELECT item.value ORDER BY item.value",
		testTableFunctionResolver{},
		[]interface{}{int64(2), int64(7)},
		hatSql.SQLQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	for index, want := range []int64{7, 8} {
		if got := result.Rows[index]["value"]; got != want {
			t.Fatalf("result.Rows[%d][value] = %#v, want %d", index, got, want)
		}
	}
}

type testTableFunctionResolver struct{}

func (testTableFunctionResolver) ResolveSQLSource(kind, key string) ([]hatSql.SQLRow, error) {
	return nil, fmt.Errorf("unexpected SQL source %s(%q)", kind, key)
}

func (testTableFunctionResolver) ResolveSQLTableFunction(name string, arguments []interface{}) ([]hatSql.SQLRow, error) {
	if name != "series" {
		return nil, fmt.Errorf("unexpected table function %q", name)
	}
	if len(arguments) != 2 {
		return nil, fmt.Errorf("unexpected arguments %#v", arguments)
	}
	count, countOK := arguments[0].(int64)
	start, startOK := arguments[1].(int64)
	if !countOK || !startOK || count < 0 {
		return nil, fmt.Errorf("unexpected arguments %#v", arguments)
	}
	rows := make([]hatSql.SQLRow, count)
	for index := range rows {
		rows[index] = hatSql.SQLRow{"value": start + int64(index)}
	}
	return rows, nil
}
