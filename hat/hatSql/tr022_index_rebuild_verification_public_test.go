package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLIndexRebuildVerificationPublicAPI(t *testing.T) {
	request := hatSql.SQLIndexRebuildRequest{
		ID:   "public",
		Name: "orders",
		Run: func(context.Context, hatSql.SQLIndexRebuildProgressFunc) error {
			return nil
		},
		Verify: func(context.Context) error { return nil },
	}
	if request.Verify == nil {
		t.Fatal("Verify callback was not retained")
	}
}
