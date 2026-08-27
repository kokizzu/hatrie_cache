package hatSql

import (
	"context"
	"strings"
	"testing"
)

type planGuardResolver struct{}

func (planGuardResolver) ResolveSQLSource(_, _ string) ([]SQLRow, error) {
	return []SQLRow{{"id": int64(1), "name": "Ada"}}, nil
}

func (planGuardResolver) ResolveSQLIndexedSource(_, _, field string, value interface{}) ([]SQLRow, bool, error) {
	if field != "id" || value != int64(1) {
		return nil, false, nil
	}
	return []SQLRow{{"id": int64(1), "name": "Ada"}}, true, nil
}

func TestVerifyPlanGuardsRequiresExpectedIndexPlan(t *testing.T) {
	resolver := planGuardResolver{}
	guard := PlanGuard{
		Name:        "people by id",
		Query:       "FROM CACHE('people') AS p WHERE p.id = 1 SELECT p.name",
		RequireNode: "INDEX SCAN",
	}
	if err := VerifyPlanGuards(context.Background(), resolver, SQLQueryOptions{}, []PlanGuard{guard}); err != nil {
		t.Fatalf("VerifyPlanGuards() error = %v", err)
	}
}

func TestVerifyPlanGuardsReportsMissingRequirement(t *testing.T) {
	err := VerifyPlanGuards(context.Background(), planGuardResolver{}, SQLQueryOptions{}, []PlanGuard{{
		Name:        "missing node",
		Query:       "FROM CACHE('people') AS p WHERE p.id = 1 SELECT p.name",
		RequireNode: "BITMAP INDEX SCAN",
	}})
	if err == nil || !strings.Contains(err.Error(), "missing required plan node") {
		t.Fatalf("VerifyPlanGuards() error = %v, want missing-node diagnostic", err)
	}
}
