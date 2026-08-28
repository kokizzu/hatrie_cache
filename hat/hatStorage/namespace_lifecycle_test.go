package hatStorage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
	"hatrie_cache/hat/hatStorage"
)

func TestNamespaceLifecycleFreezeExpireArchiveAndDelete(t *testing.T) {
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLNamespaceAdapter{
		NamespaceName: "eu",
		Store:         testEngine{},
		Resolver: hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	archived, deleted := 0, 0
	controller, err := hatStorage.NewNamespaceLifecycleController(registry, map[string]hatStorage.NamespaceLifecyclePolicy{
		"eu": {
			Archive: func(context.Context, string, hatStorage.SQLAdapter) error { archived++; return nil },
			Delete:  func(context.Context, string, hatStorage.SQLAdapter) error { deleted++; return nil },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Freeze("eu"); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Execute(context.Background(), "eu", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); !errors.Is(err, hatStorage.ErrNamespaceFrozen) {
		t.Fatalf("frozen Execute() error = %v", err)
	}
	if err := controller.Unfreeze("eu"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Archive(context.Background(), "eu"); err != nil || archived != 1 {
		t.Fatalf("Archive() = %v, calls = %d", err, archived)
	}
	if _, err := controller.Execute(context.Background(), "eu", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); !errors.Is(err, hatStorage.ErrNamespaceArchived) {
		t.Fatalf("archived Execute() error = %v", err)
	}
	if err := controller.Delete(context.Background(), "eu"); err != nil || deleted != 1 {
		t.Fatalf("Delete() = %v, calls = %d", err, deleted)
	}
	if _, err := controller.Execute(context.Background(), "eu", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); !errors.Is(err, hatStorage.ErrNamespaceDeleted) {
		t.Fatalf("deleted Execute() error = %v", err)
	}
}

func TestNamespaceLifecycleTTLExpiresToArchive(t *testing.T) {
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLNamespaceAdapter{NamespaceName: "expired", Store: testEngine{}, Resolver: hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return nil, nil })})
	if err != nil {
		t.Fatal(err)
	}
	archived := 0
	controller, err := hatStorage.NewNamespaceLifecycleController(registry, map[string]hatStorage.NamespaceLifecyclePolicy{
		"expired": {ExpiresAt: time.Now().Add(-time.Second), ExpiryAction: hatStorage.NamespaceExpiryArchive, Archive: func(context.Context, string, hatStorage.SQLAdapter) error { archived++; return nil }},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Expire(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if archived != 1 {
		t.Fatalf("archive calls = %d", archived)
	}
	if _, err := controller.Execute(context.Background(), "expired", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{}); !errors.Is(err, hatStorage.ErrNamespaceArchived) {
		t.Fatalf("expired Execute() error = %v", err)
	}
}

func TestNamespaceLifecycleQuotaQueuesAndCancels(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLNamespaceAdapter{NamespaceName: "busy", Store: testEngine{}, Resolver: hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) { close(started); <-release; return nil, nil })})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := hatStorage.NewNamespaceLifecycleController(registry, map[string]hatStorage.NamespaceLifecyclePolicy{"busy": {MaxConcurrentQueries: 1}})
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		_, err := controller.Execute(context.Background(), "busy", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{})
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first query did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = controller.Execute(ctx, "busy", "SELECT * FROM CACHE('items')", nil, hatSql.SQLQueryOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued query error = %v", err)
	}
	close(release)
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first query did not finish")
	}
}
