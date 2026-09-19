package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestM049CatalogMigrationOrdersDependenciesDeterministically(t *testing.T) {
	plan := CatalogMigrationPlan{
		Version: CatalogMigrationPlanVersion,
		Steps: []CatalogMigrationStep{
			{ID: "create-view", Object: "orders_view", Action: "create", DependsOn: []string{"create-table"}},
			{ID: "create-table", Object: "orders", Action: "create"},
			{ID: "grant-read", Object: "orders_view", Action: "grant", DependsOn: []string{"create-view"}},
			{ID: "create-audit", Object: "audit", Action: "create"},
		},
	}

	ordered, err := plan.OrderedSteps()
	if err != nil {
		t.Fatalf("OrderedSteps() error = %v", err)
	}
	ids := make([]string, 0, len(ordered))
	for _, step := range ordered {
		ids = append(ids, step.ID)
	}
	want := []string{"create-audit", "create-table", "create-view", "grant-read"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("ordered IDs = %v, want %v", ids, want)
	}
}

func TestM049CatalogMigrationRejectsInvalidGraphs(t *testing.T) {
	cases := []struct {
		name string
		plan CatalogMigrationPlan
	}{
		{
			name: "missing dependency",
			plan: CatalogMigrationPlan{Version: CatalogMigrationPlanVersion, Steps: []CatalogMigrationStep{
				{ID: "child", Object: "view", Action: "create", DependsOn: []string{"missing"}},
			}},
		},
		{
			name: "cycle",
			plan: CatalogMigrationPlan{Version: CatalogMigrationPlanVersion, Steps: []CatalogMigrationStep{
				{ID: "a", Object: "a", Action: "alter", DependsOn: []string{"b"}},
				{ID: "b", Object: "b", Action: "alter", DependsOn: []string{"a"}},
			}},
		},
		{
			name: "duplicate ID",
			plan: CatalogMigrationPlan{Version: CatalogMigrationPlanVersion, Steps: []CatalogMigrationStep{
				{ID: "same", Object: "a", Action: "create"},
				{ID: "same", Object: "b", Action: "create"},
			}},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.plan.OrderedSteps(); !errors.Is(err, ErrCatalogMigrationPlanInvalid) {
				t.Fatalf("OrderedSteps() error = %v, want ErrCatalogMigrationPlanInvalid", err)
			}
		})
	}
}

func TestM049CatalogMigrationRollsBackAppliedStepsInReverseOrder(t *testing.T) {
	applyErr := errors.New("apply index failed")
	events := make([]string, 0, 8)
	runner := CatalogMigrationRunner{
		ApplyStep: func(_ context.Context, step CatalogMigrationStep) error {
			events = append(events, "apply:"+step.ID)
			if step.ID == "create-index" {
				return applyErr
			}
			return nil
		},
		RollbackStep: func(_ context.Context, step CatalogMigrationStep) error {
			events = append(events, "rollback:"+step.ID)
			return nil
		},
	}
	plan := CatalogMigrationPlan{
		Version: CatalogMigrationPlanVersion,
		Steps: []CatalogMigrationStep{
			{ID: "create-table", Object: "orders", Action: "create"},
			{ID: "create-index", Object: "orders_idx", Action: "create-index", DependsOn: []string{"create-table"}},
		},
	}

	result, err := runner.Apply(context.Background(), plan)
	if !errors.Is(err, applyErr) || !errors.Is(err, ErrCatalogMigrationApplyFailed) {
		t.Fatalf("Apply() error = %v, want apply and sentinel errors", err)
	}
	if result.FailedStep != "create-index" || !reflect.DeepEqual(result.Applied, []string{"create-table"}) || !reflect.DeepEqual(result.RolledBack, []string{"create-table"}) {
		t.Fatalf("result = %#v, want failed index and reverse rollback", result)
	}
	wantEvents := []string{"apply:create-table", "apply:create-index", "rollback:create-table"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
}

func TestM049CatalogMigrationCancellationStillRollsBack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rolledBack := false
	runner := CatalogMigrationRunner{
		ApplyStep: func(_ context.Context, step CatalogMigrationStep) error {
			if step.ID == "second" {
				cancel()
				return errors.New("step failed")
			}
			return nil
		},
		RollbackStep: func(_ context.Context, step CatalogMigrationStep) error {
			rolledBack = true
			return nil
		},
	}
	plan := CatalogMigrationPlan{
		Version: CatalogMigrationPlanVersion,
		Steps: []CatalogMigrationStep{
			{ID: "first", Object: "first", Action: "create"},
			{ID: "second", Object: "second", Action: "create", DependsOn: []string{"first"}},
		},
	}
	if _, err := runner.Apply(ctx, plan); err == nil || !rolledBack {
		t.Fatalf("Apply() error = %v, rolledBack = %v, want failure with rollback", err, rolledBack)
	}
}
