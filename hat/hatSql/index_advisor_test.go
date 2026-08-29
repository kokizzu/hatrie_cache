package hatSql

import (
	"context"
	"testing"
	"time"
)

func TestSQLIndexAdvisorRecommendsUnindexedSlowPredicate(t *testing.T) {
	advisor := NewSQLIndexAdvisor(8)
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1), "name": "Ada"}, {"id": int64(2), "name": "Lin"}}, nil
	})
	_, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('people') AS person WHERE person.id >= 2 SELECT person.name", resolver, nil, QueryOptions{
		SlowQueryThreshold: time.Nanosecond,
		IndexAdvisor:       advisor,
	})
	if err != nil {
		t.Fatal(err)
	}
	recommendations := advisor.Recommendations()
	if len(recommendations) != 1 || recommendations[0].Key != "people" || recommendations[0].Field != "id" || recommendations[0].SlowQueries != 1 {
		t.Fatalf("Recommendations() = %#v", recommendations)
	}
}
