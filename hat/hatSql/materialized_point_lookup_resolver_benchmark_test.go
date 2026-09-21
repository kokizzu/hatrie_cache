package hatSql_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var m218MaterializedPointLookupBenchmarkSink hatSql.QueryResult

func BenchmarkM218MaterializedPointLookupPlanner(b *testing.B) {
	views, pointResolver, scanResolver := newM218MaterializedPointLookupBenchmarkFixture(b)
	_ = views
	query := `
FROM EXTERNAL('people_view') AS person
WHERE person.id = 9000
SELECT person.id, person.name`
	for _, test := range []struct {
		name     string
		resolver hatSql.SourceResolver
	}{
		{name: "point_lookup", resolver: pointResolver},
		{name: "arrangement_scan", resolver: scanResolver},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := hatSql.ExecuteQueryParameters(context.Background(), query, test.resolver, nil, hatSql.QueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				m218MaterializedPointLookupBenchmarkSink = result
			}
		})
	}
}

type m218MaterializedPointLookupScanResolver struct {
	views *hatSql.MaterializedViews
}

func (resolver m218MaterializedPointLookupScanResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "EXTERNAL" || key != "people_view" {
		return nil, nil
	}
	view, found := resolver.views.Get("people_view")
	if !found {
		return nil, fmt.Errorf("people_view is missing")
	}
	return view.Result.Rows, nil
}

func (resolver m218MaterializedPointLookupScanResolver) ResolveSQLExternalSource(key string) ([]hatSql.Row, error) {
	return resolver.ResolveSQLSource("EXTERNAL", key)
}

func newM218MaterializedPointLookupBenchmarkFixture(b *testing.B) (*hatSql.MaterializedViews, hatSql.SourceResolver, hatSql.SourceResolver) {
	b.Helper()
	rows := make([]hatSql.Row, 10000)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":   int64(index),
			"name": fmt.Sprintf("person-%05d", index),
		}
	}
	sourceResolver := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		if name == "CACHE" && key == "people" {
			return hatSql.CloneRows(rows), nil
		}
		return nil, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
	}, sourceResolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	if err := views.CreatePointLookupIndex(hatSql.MaterializedViewPointLookupDefinition{
		Name:     "people_by_id",
		ViewName: "people_view",
		Key: func(row hatSql.Row) (string, error) {
			id, ok := row["id"].(int64)
			if !ok {
				return "", fmt.Errorf("id is not int64")
			}
			return strconv.FormatInt(id, 10), nil
		},
	}); err != nil {
		b.Fatal(err)
	}
	pointResolver, err := hatSql.NewMaterializedViewPointLookupResolver(views, hatSql.MaterializedViewPointLookupSourceDefinition{
		IndexName: "people_by_id",
		Field:     "id",
		ValueKey: func(value interface{}) (string, error) {
			id, ok := value.(int64)
			if !ok {
				return "", fmt.Errorf("id is not int64")
			}
			return strconv.FormatInt(id, 10), nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return views, pointResolver, m218MaterializedPointLookupScanResolver{views: views}
}
