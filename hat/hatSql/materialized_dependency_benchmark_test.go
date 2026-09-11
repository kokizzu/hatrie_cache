package hatSql

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkMZ042MaterializedRefreshChanged(b *testing.B) {
	for _, viewCount := range []int{8, 1024, 4096} {
		for _, changed := range []struct {
			name string
			key  string
		}{
			{name: "one-match", key: "source-0000"},
			{name: "no-match", key: "source-missing"},
		} {
			b.Run(viewCountNameMZ042(viewCount)+"/"+changed.name, func(b *testing.B) {
				resolver := SourceResolverFunc(func(_ string, key string) ([]Row, error) {
					return []Row{{"value": key}}, nil
				})
				views := NewMaterializedViews()
				for index := 0; index < viewCount; index++ {
					key := viewKeyMZ042(index)
					definition := MaterializedViewDefinition{
						Name:         "view-" + key,
						Query:        "FROM CACHE('" + key + "') SELECT value",
						Dependencies: []string{key},
					}
					if _, err := views.Create(context.Background(), definition, resolver, QueryOptions{}); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					statuses, err := views.RefreshChanged(context.Background(), []string{changed.key}, resolver, QueryOptions{})
					if err != nil {
						b.Fatal(err)
					}
					if changed.name == "one-match" && len(statuses) != 1 {
						b.Fatalf("refreshed statuses = %d, want 1", len(statuses))
					}
					if changed.name == "no-match" && len(statuses) != 0 {
						b.Fatalf("refreshed statuses = %d, want 0", len(statuses))
					}
				}
			})
		}
	}
}

func viewCountNameMZ042(count int) string {
	return "views-" + fmt.Sprintf("%04d", count)
}

func viewKeyMZ042(index int) string {
	return fmt.Sprintf("source-%04d", index)
}
