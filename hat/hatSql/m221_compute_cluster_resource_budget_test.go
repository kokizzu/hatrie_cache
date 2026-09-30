package hatSql_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestM221ComputeClusterMemoryBudgetIsIndependentAndBounded(t *testing.T) {
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeClusters: map[string]hatSql.SQLComputeClusterOptions{
			"analytics": {
				Workers:                   2,
				QueueCapacity:             4,
				MemoryBudgetBytes:         100,
				MemoryAdmissionMaxPending: 2,
			},
			"serving": {
				Workers:           1,
				MemoryBudgetBytes: 50,
			},
		},
	})
	defer manager.Close()

	resolver := &m221BlockingResolver{started: make(chan struct{}), release: make(chan struct{})}
	firstDone := make(chan error, 1)
	go func() {
		_, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{
			ComputeCluster:         "analytics",
			MemoryReservationBytes: 80,
		})
		firstDone <- err
	}()
	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		t.Fatal("first query did not reach the resolver")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{
			ComputeCluster:         "analytics",
			MemoryReservationBytes: 30,
		})
		secondDone <- err
	}()

	stats := m221WaitForClusterStats(t, manager, "analytics", func(stats hatSql.SQLMemoryAdmissionStats) bool {
		return stats.ActiveBytes == 80 && stats.Pending == 1
	})
	if stats.MaxBytes != 100 || stats.MaxPending != 2 {
		t.Fatalf("analytics resource stats = %#v", stats)
	}

	servingStats, err := manager.ComputeClusterMemoryStats("serving")
	if err != nil {
		t.Fatalf("serving stats error = %v", err)
	}
	if servingStats.MaxBytes != 50 || servingStats.ActiveBytes != 0 || servingStats.Pending != 0 {
		t.Fatalf("serving resource stats = %#v", servingStats)
	}

	close(resolver.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first query error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second query error = %v", err)
	}
	finalStats, err := manager.ComputeClusterMemoryStats("analytics")
	if err != nil {
		t.Fatalf("final analytics stats error = %v", err)
	}
	if finalStats.ActiveBytes != 0 || finalStats.Pending != 0 || finalStats.Admitted != 2 || finalStats.Waited != 1 {
		t.Fatalf("final analytics stats = %#v", finalStats)
	}
}

func TestM221BudgetedClusterRequiresMemoryReservation(t *testing.T) {
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeClusters: map[string]hatSql.SQLComputeClusterOptions{
			"analytics": {Workers: 1, MemoryBudgetBytes: 100},
		},
	})
	defer manager.Close()

	_, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	}), nil, hatSql.QueryOptions{ComputeCluster: "analytics"})
	if !errors.Is(err, hatSql.ErrSQLQueryManagerComputeClusterMemoryReservationRequired) {
		t.Fatalf("missing reservation error = %v, want %v", err, hatSql.ErrSQLQueryManagerComputeClusterMemoryReservationRequired)
	}
}

func TestM221ComputeClusterBudgetValidation(t *testing.T) {
	for name, options := range map[string]hatSql.SQLComputeClusterOptions{
		"negative budget":  {Workers: 1, MemoryBudgetBytes: -1},
		"negative pending": {Workers: 1, MemoryAdmissionMaxPending: -1},
	} {
		t.Run(name, func(t *testing.T) {
			manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
				ComputeClusters: map[string]hatSql.SQLComputeClusterOptions{"analytics": options},
			})
			defer manager.Close()
			_, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", nil, nil, hatSql.QueryOptions{
				ComputeCluster: "analytics",
			})
			if !errors.Is(err, hatSql.ErrSQLQueryManagerComputeClusterInvalid) {
				t.Fatalf("validation error = %v, want %v", err, hatSql.ErrSQLQueryManagerComputeClusterInvalid)
			}
		})
	}
}

type m221BlockingResolver struct {
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func (resolver *m221BlockingResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	resolver.startOnce.Do(func() { close(resolver.started) })
	<-resolver.release
	return []hatSql.Row{{"id": int64(1)}}, nil
}

func m221WaitForClusterStats(t *testing.T, manager *hatSql.SQLQueryManager, name string, ready func(hatSql.SQLMemoryAdmissionStats) bool) hatSql.SQLMemoryAdmissionStats {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats, err := manager.ComputeClusterMemoryStats(name)
		if err != nil {
			t.Fatalf("cluster stats error = %v", err)
		}
		if ready(stats) {
			return stats
		}
		time.Sleep(time.Millisecond)
	}
	stats, err := manager.ComputeClusterMemoryStats(name)
	if err != nil {
		t.Fatalf("final cluster stats error = %v", err)
	}
	t.Fatalf("cluster stats did not reach expected state: %#v", stats)
	return hatSql.SQLMemoryAdmissionStats{}
}
