package hatSql_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM049CatalogMigrationPublicContract(t *testing.T) {
	plan := hatSql.CatalogMigrationPlan{
		Version: hatSql.CatalogMigrationPlanVersion,
		Steps: []hatSql.CatalogMigrationStep{
			{ID: "create-table", Object: "users", Action: "create"},
			{ID: "create-index", Object: "users_email", Action: "create", DependsOn: []string{"create-table"}},
		},
	}

	ordered, err := plan.OrderedSteps()
	if err != nil {
		t.Fatalf("order migration plan: %v", err)
	}
	if got := ordered[1].ID; got != "create-index" {
		t.Fatalf("ordered step = %q, want create-index", got)
	}

	var applied []string
	result, err := (hatSql.CatalogMigrationRunner{
		ApplyStep: func(_ context.Context, step hatSql.CatalogMigrationStep) error {
			applied = append(applied, step.ID)
			return nil
		},
		RollbackStep: func(context.Context, hatSql.CatalogMigrationStep) error { return nil },
	}).Apply(context.Background(), plan)
	if err != nil {
		t.Fatalf("apply migration plan: %v", err)
	}
	if len(result.Applied) != 2 || len(applied) != 2 {
		t.Fatalf("applied result = %#v, callback calls = %#v", result.Applied, applied)
	}
}

func TestM049CatalogMigrationPublicErrors(t *testing.T) {
	_, err := (hatSql.CatalogMigrationRunner{}).Apply(context.Background(), hatSql.CatalogMigrationPlan{})
	if !errors.Is(err, hatSql.ErrCatalogMigrationRunnerRequired) {
		t.Fatalf("error = %v, want ErrCatalogMigrationRunnerRequired", err)
	}
}
