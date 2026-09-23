package hatSql_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM220DropPointLookupFieldsPreservesOtherIndexes(t *testing.T) {
	resolver := &m220IndexLifecycleResolver{
		rows: []hatSql.Row{
			{"id": int64(1), "region": "sg", "name": "Ada"},
			{"id": int64(2), "region": "jp", "name": "Kai"},
		},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:              "people_view",
		Query:             "FROM CACHE('people') AS p SELECT p.id, p.region, p.name",
		Dependencies:      []string{"people"},
		PointLookupFields: []string{"id", "region"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := views.DropPointLookupFields("people_view", "region"); err != nil {
		t.Fatal(err)
	}
	rows, available, err := views.PointLookup("people_view", "region", "sg")
	if err != nil || available || rows != nil {
		t.Fatalf("removed region index = %#v, %v, %v; want unavailable", rows, available, err)
	}
	rows, available, err = views.PointLookup("people_view", "id", int64(2))
	if err != nil || !available || !reflect.DeepEqual(rows, []hatSql.Row{{"id": int64(2), "region": "jp", "name": "Kai"}}) {
		t.Fatalf("remaining id index = %#v, %v, %v; want Kai", rows, available, err)
	}

	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('people') AS p WHERE p.region = 'sg' SELECT p.id, p.region, p.name", resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("arrangement fallback rows = %#v, want Ada", result.Rows)
	}
	if len(result.Plan) != 1 || result.Plan[0].Node != "MATERIALIZED ARRANGEMENT SCAN" {
		t.Fatalf("arrangement fallback plan = %#v, want materialized arrangement scan", result.Plan)
	}

	if err := views.DropPointLookupFields("people_view"); err != nil {
		t.Fatal(err)
	}
	if rows, available, err := views.PointLookup("people_view", "id", int64(2)); err != nil || available || rows != nil {
		t.Fatalf("remove all indexes = %#v, %v, %v; want unavailable", rows, available, err)
	}
}

func TestM220PointLookupReadersRemainSafeDuringIndexRemoval(t *testing.T) {
	rows := make([]hatSql.Row, 512)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":     int64(index),
			"region": "sg",
			"name":   fmt.Sprintf("person-%d", index),
		}
	}
	resolver := &m220IndexLifecycleResolver{rows: rows, version: "1"}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:              "people_view",
		Query:             "FROM CACHE('people') AS p SELECT p.id, p.region, p.name",
		Dependencies:      []string{"people"},
		PointLookupFields: []string{"region"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 1)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < 100; attempt++ {
				rows, available, err := views.PointLookup("people_view", "region", "sg")
				if err != nil {
					select {
					case errors <- err:
					default:
					}
					return
				}
				if available && len(rows) != len(resolver.rows) {
					select {
					case errors <- fmt.Errorf("reader returned %d rows, want %d", len(rows), len(resolver.rows)):
					default:
					}
					return
				}
			}
		}()
	}
	if err := views.DropPointLookupFields("people_view", "region"); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
}

type m220IndexLifecycleResolver struct {
	rows    []hatSql.Row
	version string
}

func (resolver *m220IndexLifecycleResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	return hatSql.CloneRows(resolver.rows), nil
}

func (resolver *m220IndexLifecycleResolver) SQLSourceVersion(name, key string) (string, bool, error) {
	if name != "CACHE" || key != "people" {
		return "", false, nil
	}
	return resolver.version, true, nil
}
