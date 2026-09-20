package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

type m090bContextResolver struct {
	started     chan struct{}
	contextSeen context.Context
	legacyCalls int
}

type m090bContextKey struct{}

func newM090bContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.WithValue(context.Background(), m090bContextKey{}, "m090b"))
}

func (resolver *m090bContextResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.legacyCalls++
	return []Row{{"id": int64(1)}}, nil
}

func (resolver *m090bContextResolver) ResolveSQLSourceContext(ctx context.Context, _ string, _ string) ([]Row, error) {
	resolver.contextSeen = ctx
	close(resolver.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

var _ SourceResolver = (*m090bContextResolver)(nil)
var _ ContextSourceResolver = (*m090bContextResolver)(nil)

func TestM090bContextSourceResolverPropagatesCancellation(t *testing.T) {
	ctx, cancel := newM090bContext()
	defer cancel()
	resolver := &m090bContextResolver{started: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := ExecuteSQLQueryContext(ctx, "FROM CACHE('events') SELECT id", resolver, SQLQueryOptions{})
		result <- err
	}()

	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		t.Fatal("context-aware source resolver was not called")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ExecuteSQLQueryContext() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("query did not stop after source cancellation")
	}
	if resolver.contextSeen == nil || resolver.contextSeen.Value(m090bContextKey{}) != "m090b" {
		t.Fatal("context-aware source resolver did not receive the query context values")
	}
	if resolver.legacyCalls != 0 {
		t.Fatalf("legacy source resolver calls = %d, want 0", resolver.legacyCalls)
	}
}

func TestM090bLegacySourceResolverStillWorks(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') SELECT id", SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(7)}}, nil
	}), SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(7) {
		t.Fatalf("ExecuteSQLQueryContext() rows = %#v, want one legacy row", result.Rows)
	}
}

func TestM090bContextSourceResolverForwardsThroughWrappers(t *testing.T) {
	tests := []struct {
		name string
		wrap func(*m090bContextResolver) SQLSourceResolver
	}{
		{name: "session", wrap: func(resolver *m090bContextResolver) SQLSourceResolver {
			return NewSQLSession(resolver)
		}},
		{name: "catalog", wrap: func(resolver *m090bContextResolver) SQLSourceResolver {
			return CatalogResolver{Source: resolver}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := newM090bContext()
			defer cancel()
			base := &m090bContextResolver{started: make(chan struct{})}
			resolver := test.wrap(base)
			result := make(chan error, 1)
			go func() {
				_, err := ExecuteSQLQueryContext(ctx, "FROM CACHE('events') SELECT id", resolver, SQLQueryOptions{})
				result <- err
			}()

			select {
			case <-base.started:
			case <-time.After(time.Second):
				t.Fatal("context-aware source resolver was not called through wrapper")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("ExecuteSQLQueryContext() error = %v, want context.Canceled", err)
				}
			case <-time.After(time.Second):
				t.Fatal("wrapped query did not stop after source cancellation")
			}
			if base.contextSeen == nil || base.contextSeen.Value(m090bContextKey{}) != "m090b" {
				t.Fatal("wrapper did not forward the query context values")
			}
			if base.legacyCalls != 0 {
				t.Fatalf("legacy source resolver calls = %d, want 0", base.legacyCalls)
			}
		})
	}
}

type m090bFastContextResolver struct{}

func (m090bFastContextResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (m090bFastContextResolver) ResolveSQLSourceContext(context.Context, string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func BenchmarkM090bLegacySourceResolver(b *testing.B) {
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1)}}, nil
	})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') SELECT id", resolver, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM090bContextSourceResolver(b *testing.B) {
	resolver := m090bFastContextResolver{}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') SELECT id", resolver, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
