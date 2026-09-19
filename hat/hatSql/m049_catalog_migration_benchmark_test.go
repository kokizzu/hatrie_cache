package hatSql

import (
	"context"
	"testing"
)

func BenchmarkM049ApplyCatalogMigration(b *testing.B) {
	plan := CatalogMigrationPlan{
		Version: CatalogMigrationPlanVersion,
		Steps: []CatalogMigrationStep{
			{ID: "create-audit", Object: "audit", Action: "create"},
			{ID: "create-table", Object: "orders", Action: "create"},
			{ID: "create-view", Object: "orders_view", Action: "create", DependsOn: []string{"create-table"}},
			{ID: "grant-read", Object: "orders_view", Action: "grant", DependsOn: []string{"create-view"}},
		},
	}
	runner := CatalogMigrationRunner{
		ApplyStep:    func(context.Context, CatalogMigrationStep) error { return nil },
		RollbackStep: func(context.Context, CatalogMigrationStep) error { return nil },
	}
	ctx := context.Background()
	b.ReportAllocs()
	checksum := 0
	for i := 0; i < b.N; i++ {
		result, err := runner.Apply(ctx, plan)
		if err != nil {
			b.Fatal(err)
		}
		checksum += len(result.Applied)
	}
	if checksum == 0 {
		b.Fatal("unexpected zero checksum")
	}
}
