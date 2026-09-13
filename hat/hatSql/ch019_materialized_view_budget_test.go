package hatSql_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCH019MaterializedViewsStorageBudgetRejectsRefreshAtomically(t *testing.T) {
	rows := []hatSql.Row{{"name": "Ada"}}
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows), nil
	})
	views, err := hatSql.NewMaterializedViewsWithOptions(hatSql.MaterializedViewsOptions{MaxRows: 1})
	if err != nil {
		t.Fatalf("NewMaterializedViewsWithOptions() error = %v", err)
	}
	definition := hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}
	if _, err := views.Create(context.Background(), definition, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	rows = []hatSql.Row{{"name": "Ada"}, {"name": "Lin"}}
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); !errors.Is(err, hatSql.ErrMaterializedViewBudgetExceeded) {
		t.Fatalf("RefreshChanged() error = %v, want ErrMaterializedViewBudgetExceeded", err)
	}
	view, ok := views.Get("people_view")
	if !ok || view.Status.Revision != 1 || len(view.Result.Rows) != 1 || view.Result.Rows[0]["name"] != "Ada" {
		t.Fatalf("view after rejected refresh = %#v, want original snapshot", view)
	}
	usage := views.Usage()
	if usage.Rows != 1 || usage.MaxRows != 1 {
		t.Fatalf("Usage() = %#v, want one retained row and max rows 1", usage)
	}
}

func TestCH019MaterializedViewsStorageBudgetRejectsOversizedCreate(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": "Ada"}}, nil
	})
	views, err := hatSql.NewMaterializedViewsWithOptions(hatSql.MaterializedViewsOptions{MaxBytes: 1})
	if err != nil {
		t.Fatalf("NewMaterializedViewsWithOptions() error = %v", err)
	}
	_, err = views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{})
	if !errors.Is(err, hatSql.ErrMaterializedViewBudgetExceeded) {
		t.Fatalf("Create() error = %v, want ErrMaterializedViewBudgetExceeded", err)
	}
	if usage := views.Usage(); usage.Rows != 0 || usage.Bytes != 0 {
		t.Fatalf("Usage() after rejected create = %#v, want empty", usage)
	}
}

func BenchmarkCH019MaterializedViewsStorageAdmission(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": "Ada"}, {"name": "Lin"}}, nil
	})
	definition := hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}
	for _, benchmark := range []struct {
		name    string
		options hatSql.MaterializedViewsOptions
	}{
		{name: "unbounded", options: hatSql.MaterializedViewsOptions{}},
		{name: "bounded", options: hatSql.MaterializedViewsOptions{MaxRows: 16, MaxBytes: 1 << 20}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			views, err := hatSql.NewMaterializedViewsWithOptions(benchmark.options)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := views.Create(context.Background(), definition, resolver, hatSql.QueryOptions{}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
