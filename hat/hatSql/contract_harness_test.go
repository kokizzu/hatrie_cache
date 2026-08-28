package hatSql

import (
	"context"
	"testing"
)

func TestRunSQLContracts(t *testing.T) {
	err := RunSQLContracts(context.Background(), SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) { return []SQLRow{{"id": 1}}, nil }), []SQLContract{{Name: "one", Query: "SELECT id FROM CACHE('items')", Assert: RequireExactlyOne}})
	if err != nil {
		t.Fatal(err)
	}
}
