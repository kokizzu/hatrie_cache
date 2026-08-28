package hatSql

import (
	"context"
	"fmt"
)

// SQLContract is a portable application-level SQL assertion usable directly
// from external repositories' Go test suites.
type SQLContract struct {
	Name, Query string
	Parameters  []interface{}
	Options     SQLQueryOptions
	Assert      func(SQLQueryResult) (SQLRow, error)
}

func RunSQLContracts(ctx context.Context, resolver SQLSourceResolver, contracts []SQLContract) error {
	for _, contract := range contracts {
		result, err := ExecuteSQLQueryParameters(ctx, contract.Query, resolver, contract.Parameters, contract.Options)
		if err != nil {
			return fmt.Errorf("SQL contract %q: %w", contract.Name, err)
		}
		if contract.Assert != nil {
			if _, err := contract.Assert(result); err != nil {
				return fmt.Errorf("SQL contract %q: %w", contract.Name, err)
			}
		}
	}
	return nil
}
