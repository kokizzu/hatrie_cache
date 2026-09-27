package hatSql_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCH011RefreshQueueCoalescesNotifications(t *testing.T) {
	current := "Ada"
	resolver := hatSql.SourceResolverFunc(func(_ string, _ string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": current}}, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	queue, err := hatSql.NewMaterializedViewRefreshQueue(views, resolver, hatSql.QueryOptions{}, hatSql.MaterializedViewRefreshQueueOptions{})
	if err != nil {
		t.Fatalf("NewMaterializedViewRefreshQueue() error = %v", err)
	}
	if err := queue.Notify(context.Background(), []string{"people", "people"}, hatSql.MaterializedViewRefreshMetadata{IdempotencyKeys: []string{"event-2"}}); err != nil {
		t.Fatalf("first Notify() error = %v", err)
	}
	current = "Lin"
	if err := queue.Notify(context.Background(), []string{"people"}, hatSql.MaterializedViewRefreshMetadata{IdempotencyKeys: []string{"event-1"}}); err != nil {
		t.Fatalf("second Notify() error = %v", err)
	}
	statuses, processed, err := queue.RunOnce(context.Background())
	if err != nil || !processed || len(statuses) != 1 {
		t.Fatalf("RunOnce() = %#v, %t, %v", statuses, processed, err)
	}
	if statuses[0].Revision != 2 || !reflect.DeepEqual(statuses[0].IdempotencyKeys, []string{"event-1", "event-2"}) {
		t.Fatalf("status = %#v", statuses[0])
	}
	view, ok := views.Get("people_view")
	if !ok || view.Result.Rows[0]["name"] != "Lin" {
		t.Fatalf("view = %#v, exists=%t", view, ok)
	}
	stats := queue.Stats()
	if stats.Notifications != 2 || stats.CoalescedNotifications == 0 || stats.RefreshBatches != 1 || stats.RefreshedViews != 1 || stats.PendingSources != 0 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestCH011RefreshQueueRequeuesFailedBatch(t *testing.T) {
	available := false
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		if !available {
			return nil, fmt.Errorf("source %q unavailable", key)
		}
		return []hatSql.Row{{"name": "Lin"}}, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": "Ada"}}, nil
	}), hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	queue, err := hatSql.NewMaterializedViewRefreshQueue(views, resolver, hatSql.QueryOptions{}, hatSql.MaterializedViewRefreshQueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Notify(context.Background(), []string{"people"}, hatSql.MaterializedViewRefreshMetadata{}); err != nil {
		t.Fatal(err)
	}
	if _, processed, err := queue.RunOnce(context.Background()); err == nil || !processed {
		t.Fatalf("failed RunOnce() = processed %t, error %v", processed, err)
	}
	if got := queue.Stats().PendingSources; got != 1 {
		t.Fatalf("pending sources after failure = %d, want 1", got)
	}
	available = true
	statuses, processed, err := queue.RunOnce(context.Background())
	if err != nil || !processed || len(statuses) != 1 || statuses[0].Revision != 2 {
		t.Fatalf("retry RunOnce() = %#v, %t, %v", statuses, processed, err)
	}
	if got := queue.Stats().Failures; got != 1 {
		t.Fatalf("failures = %d, want 1", got)
	}
}

func TestCH011RefreshQueueCancellationRequeuesBatch(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(_ string, _ string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": "Ada"}}, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	queue, err := hatSql.NewMaterializedViewRefreshQueue(views, resolver, hatSql.QueryOptions{}, hatSql.MaterializedViewRefreshQueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Notify(context.Background(), []string{"people"}, hatSql.MaterializedViewRefreshMetadata{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, processed, err := queue.RunOnce(ctx); !errors.Is(err, context.Canceled) || !processed {
		t.Fatalf("canceled RunOnce() = processed %t, error %v", processed, err)
	}
	if got := queue.Stats().PendingSources; got != 1 {
		t.Fatalf("pending sources after cancellation = %d, want 1", got)
	}
	statuses, processed, err := queue.RunOnce(context.Background())
	if err != nil || !processed || len(statuses) != 1 {
		t.Fatalf("retry after cancellation = %#v, %t, %v", statuses, processed, err)
	}
}

func TestCH011RefreshQueueRejectsPendingSourceOverflowAtomically(t *testing.T) {
	queue, err := hatSql.NewMaterializedViewRefreshQueue(
		hatSql.NewMaterializedViews(),
		nil,
		hatSql.QueryOptions{},
		hatSql.MaterializedViewRefreshQueueOptions{MaxPendingSources: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Notify(context.Background(), []string{"people"}, hatSql.MaterializedViewRefreshMetadata{}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Notify(context.Background(), []string{"teams"}, hatSql.MaterializedViewRefreshMetadata{}); !errors.Is(err, hatSql.ErrMaterializedViewRefreshQueueFull) {
		t.Fatalf("overflow Notify() error = %v, want %v", err, hatSql.ErrMaterializedViewRefreshQueueFull)
	}
	if got := queue.Stats().PendingSources; got != 1 {
		t.Fatalf("pending sources after overflow = %d, want 1", got)
	}
}

func TestCH011RefreshQueueRetainsNotificationArrivingDuringRefresh(t *testing.T) {
	current := "Ada"
	createResolver := hatSql.SourceResolverFunc(func(_ string, _ string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": current}}, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, createResolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	refreshResolver := hatSql.SourceResolverFunc(func(_ string, _ string) ([]hatSql.Row, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return []hatSql.Row{{"name": current}}, nil
	})
	queue, err := hatSql.NewMaterializedViewRefreshQueue(views, refreshResolver, hatSql.QueryOptions{}, hatSql.MaterializedViewRefreshQueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Notify(context.Background(), []string{"people"}, hatSql.MaterializedViewRefreshMetadata{}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, processed, runErr := queue.RunOnce(context.Background())
		if !processed && runErr == nil {
			runErr = errors.New("RunOnce() did not process the claimed batch")
		}
		result <- runErr
	}()
	<-started
	current = "Lin"
	if err := queue.Notify(context.Background(), []string{"people"}, hatSql.MaterializedViewRefreshMetadata{}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got := queue.Stats().PendingSources; got != 1 {
		t.Fatalf("pending sources after in-flight notification = %d, want 1", got)
	}
	statuses, processed, err := queue.RunOnce(context.Background())
	if err != nil || !processed || len(statuses) != 1 || statuses[0].Revision != 3 {
		t.Fatalf("second RunOnce() = %#v, %t, %v", statuses, processed, err)
	}
}

func TestCH011RefreshQueueRejectsInvalidOptions(t *testing.T) {
	_, err := hatSql.NewMaterializedViewRefreshQueue(
		hatSql.NewMaterializedViews(),
		nil,
		hatSql.QueryOptions{},
		hatSql.MaterializedViewRefreshQueueOptions{MaxPendingSources: -1},
	)
	if !errors.Is(err, hatSql.ErrMaterializedViewRefreshQueueOptionsInvalid) {
		t.Fatalf("invalid options error = %v, want %v", err, hatSql.ErrMaterializedViewRefreshQueueOptionsInvalid)
	}
}

func BenchmarkCH011RefreshQueueNotifyAndProcess(b *testing.B) {
	current := "Ada"
	resolver := hatSql.SourceResolverFunc(func(_ string, _ string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": current}}, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	queue, err := hatSql.NewMaterializedViewRefreshQueue(views, resolver, hatSql.QueryOptions{}, hatSql.MaterializedViewRefreshQueueOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		current = fmt.Sprintf("person-%d", index)
		if err := queue.Notify(context.Background(), []string{"people"}, hatSql.MaterializedViewRefreshMetadata{}); err != nil {
			b.Fatal(err)
		}
		statuses, processed, err := queue.RunOnce(context.Background())
		if err != nil || !processed {
			b.Fatalf("RunOnce() = %#v, %t, %v", statuses, processed, err)
		}
	}
}

func BenchmarkCH011RefreshQueueCoalescedBatch(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(_ string, _ string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": "Lin"}}, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	queue, err := hatSql.NewMaterializedViewRefreshQueue(views, resolver, hatSql.QueryOptions{}, hatSql.MaterializedViewRefreshQueueOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		for range 8 {
			if err := queue.Notify(context.Background(), []string{"people"}, hatSql.MaterializedViewRefreshMetadata{}); err != nil {
				b.Fatal(err)
			}
		}
		if _, processed, err := queue.RunOnce(context.Background()); err != nil || !processed {
			b.Fatalf("RunOnce() = processed %t, error %v", processed, err)
		}
	}
}
