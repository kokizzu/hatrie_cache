package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestM217MaterializedViewPointLookupMaintainsCompleteRows(t *testing.T) {
	data := []Row{
		{"id": int64(1), "region": "sg", "name": "Ada"},
		{"id": int64(2), "region": "sg", "name": "Lin"},
		{"id": int64(3), "region": "jp", "name": "Kai"},
		{"id": int64(4), "region": nil, "name": "Null"},
	}
	fail := false
	resolver := SourceResolverFunc(func(_ string, key string) ([]Row, error) {
		if key != "people" {
			t.Fatalf("source key = %q, want people", key)
		}
		if fail {
			return nil, fmt.Errorf("source refresh failed")
		}
		return CloneRows(data), nil
	})
	views := NewMaterializedViews()
	definition := MaterializedViewDefinition{
		Name:              "people_view",
		Query:             "FROM CACHE('people') SELECT id, region, name",
		Dependencies:      []string{"people"},
		PointLookupFields: []string{"id", "region"},
	}
	if _, err := views.Create(context.Background(), definition, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}

	rows, available, err := views.PointLookup("people_view", "region", "sg")
	if err != nil {
		t.Fatal(err)
	}
	if !available {
		t.Fatal("PointLookup() available = false, want true")
	}
	want := []Row{
		{"id": int64(1), "region": "sg", "name": "Ada"},
		{"id": int64(2), "region": "sg", "name": "Lin"},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("PointLookup() = %#v, want %#v", rows, want)
	}
	rows[0]["name"] = "mutated"
	rows, available, err = views.PointLookup("people_view", "region", "sg")
	if err != nil || !available || rows[0]["name"] != "Ada" {
		t.Fatalf("PointLookup() clone = %#v, %v, %v; want Ada", rows, available, err)
	}

	nullRows, available, err := views.PointLookup("people_view", "region", nil)
	if err != nil || !available || len(nullRows) != 1 || nullRows[0]["name"] != "Null" {
		t.Fatalf("NULL PointLookup() = %#v, %v, %v; want one Null row", nullRows, available, err)
	}
	if _, available, err := views.PointLookup("people_view", "name", "Ada"); err != nil || available {
		t.Fatalf("unconfigured PointLookup() = %v, %v; want unavailable", available, err)
	}

	data = []Row{
		{"id": int64(5), "region": "sg", "name": "Mira"},
		{"id": int64(6), "region": "us", "name": "Noah"},
	}
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	rows, available, err = views.PointLookup("people_view", "region", "sg")
	if err != nil || !available || !reflect.DeepEqual(rows, []Row{{"id": int64(5), "region": "sg", "name": "Mira"}}) {
		t.Fatalf("refreshed PointLookup() = %#v, %v, %v; want Mira", rows, available, err)
	}
	oldRows, available, err := views.PointLookup("people_view", "region", "jp")
	if err != nil || !available || len(oldRows) != 0 {
		t.Fatalf("stale PointLookup() = %#v, %v, %v; want empty", oldRows, available, err)
	}
	fail = true
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, QueryOptions{}); err == nil {
		t.Fatal("failed refresh error = nil, want source error")
	}
	fail = false
	rows, available, err = views.PointLookup("people_view", "region", "sg")
	if err != nil || !available || len(rows) != 1 || rows[0]["name"] != "Mira" {
		t.Fatalf("post-failure PointLookup() = %#v, %v, %v; want prior Mira snapshot", rows, available, err)
	}
}

func TestM217MaterializedViewPointLookupRejectsUnknownOutputField(t *testing.T) {
	resolver := SourceResolverFunc(func(_ string, _ string) ([]Row, error) {
		return []Row{{"id": int64(1)}}, nil
	})
	views := NewMaterializedViews()
	_, err := views.Create(context.Background(), MaterializedViewDefinition{
		Name:              "invalid_view",
		Query:             "FROM CACHE('people') SELECT id",
		Dependencies:      []string{"people"},
		PointLookupFields: []string{"missing"},
	}, resolver, QueryOptions{})
	if err == nil {
		t.Fatal("Create() error = nil, want unknown output field error")
	}
}
