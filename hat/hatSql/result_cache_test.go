package hatSql

import (
	"context"
	"testing"
)

func TestResultCacheInvalidatesByEpochAndReturnsIndependentResults(t *testing.T) {
	cache := NewResultCache(2)
	calls := 0
	execute := func(context.Context) (QueryResult, error) {
		calls++
		return QueryResult{Columns: []string{"id"}, Rows: []Row{{"id": int64(calls)}}}, nil
	}

	epoch := uint64(1)
	first, err := cache.Execute(context.Background(), "people", func() uint64 { return epoch }, execute)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	first.Rows[0]["id"] = int64(99)
	cached, err := cache.Execute(context.Background(), "people", func() uint64 { return epoch }, execute)
	if err != nil {
		t.Fatalf("cached Execute() error = %v", err)
	}
	if calls != 1 || cached.Rows[0]["id"] != float64(1) {
		t.Fatalf("cached Execute() = %#v after %d calls, want id 1 after one call", cached, calls)
	}

	epoch = 2
	updated, err := cache.Execute(context.Background(), "people", func() uint64 { return epoch }, execute)
	if err != nil {
		t.Fatalf("updated Execute() error = %v", err)
	}
	if calls != 2 || updated.Rows[0]["id"] != int64(2) {
		t.Fatalf("updated Execute() = %#v after %d calls, want id 2 after two calls", updated, calls)
	}
}

func TestResultCacheHitClonesNestedRowsAndPlanPointers(t *testing.T) {
	cache := NewResultCache(1)
	actualRows := 1
	execute := func(context.Context) (QueryResult, error) {
		return QueryResult{
			Columns: []string{"payload"},
			Rows:    []Row{{"payload": map[string]interface{}{"items": []interface{}{map[string]interface{}{"name": "original"}}}}},
			Plan:    []ExplainStep{{Node: "SCAN", ActualOutputRows: &actualRows}},
		}, nil
	}
	if _, err := cache.Execute(context.Background(), "nested", func() uint64 { return 1 }, execute); err != nil {
		t.Fatalf("seed Execute() error = %v", err)
	}
	first, err := cache.Execute(context.Background(), "nested", func() uint64 { return 1 }, execute)
	if err != nil {
		t.Fatalf("first cached Execute() error = %v", err)
	}
	first.Rows[0]["payload"].(map[string]interface{})["items"].([]interface{})[0].(map[string]interface{})["name"] = "changed"
	*first.Plan[0].ActualOutputRows = 99
	second, err := cache.Execute(context.Background(), "nested", func() uint64 { return 1 }, execute)
	if err != nil {
		t.Fatalf("second cached Execute() error = %v", err)
	}
	if got := second.Rows[0]["payload"].(map[string]interface{})["items"].([]interface{})[0].(map[string]interface{})["name"]; got != "original" {
		t.Fatalf("cached nested value = %#v, want original", got)
	}
	if got := *second.Plan[0].ActualOutputRows; got != 1 {
		t.Fatalf("cached plan output rows = %d, want 1", got)
	}
}
