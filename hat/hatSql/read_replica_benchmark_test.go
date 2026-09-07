package hatSql_test

import (
	"context"
	"errors"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkReadReplicaSetExecute(b *testing.B) {
	set, err := hatSql.NewReadReplicaSet(hatSql.SourceResolverFunc(func(_, _ string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": "ok"}}, nil
	}))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := set.Execute(context.Background(), "FROM CACHE('people') SELECT name", nil, hatSql.QueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadReplicaSetExecuteWithRetrySingleAttempt(b *testing.B) {
	set, err := hatSql.NewReadReplicaSet(hatSql.SourceResolverFunc(func(_, _ string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": "ok"}}, nil
	}))
	if err != nil {
		b.Fatal(err)
	}
	retry := hatSql.ReadReplicaRetryOptions{MaxAttempts: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := set.ExecuteWithRetry(context.Background(), "FROM CACHE('people') SELECT name", nil, hatSql.QueryOptions{}, retry); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadReplicaSetExecuteWithRetryAfterFailure(b *testing.B) {
	set, err := hatSql.NewReadReplicaSet(
		hatSql.SourceResolverFunc(func(_, _ string) ([]hatSql.Row, error) {
			return nil, errors.New("replica unavailable")
		}),
		hatSql.SourceResolverFunc(func(_, _ string) ([]hatSql.Row, error) {
			return []hatSql.Row{{"name": "ok"}}, nil
		}),
	)
	if err != nil {
		b.Fatal(err)
	}
	retry := hatSql.ReadReplicaRetryOptions{MaxAttempts: 2, Retryable: func(error) bool { return true }}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := set.ExecuteWithRetry(context.Background(), "FROM CACHE('people') SELECT name", nil, hatSql.QueryOptions{}, retry); err != nil {
			b.Fatal(err)
		}
	}
}
