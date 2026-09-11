package hatSql_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestNamespaceQueryGovernorComputePoolsAreIndependent(t *testing.T) {
	governor, err := hatSql.NewNamespaceQueryGovernor(
		hatSql.NamespaceResourceLimits{},
		map[string]hatSql.NamespaceResourceLimits{
			"east": {ComputeWorkers: 1, ComputeQueueCapacity: 1},
			"west": {ComputeWorkers: 1, ComputeQueueCapacity: 1},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := governor.Close(); err != nil {
			t.Fatalf("close governor: %v", err)
		}
	}()

	var started atomic.Int32
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		started.Add(1)
		entered <- struct{}{}
		<-release
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	results := make(chan error, 2)
	for _, namespace := range []string{"east", "west"} {
		namespace := namespace
		go func() {
			result, err := governor.Execute(context.Background(), namespace, "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{})
			if err == nil && (len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1)) {
				err = errors.New("unexpected query result")
			}
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("independent namespace compute pool did not start")
		}
	}
	if got := started.Load(); got != 2 {
		t.Fatalf("started queries = %d, want 2", got)
	}
	close(release)
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("query did not finish")
		}
	}
}

func TestNamespaceQueryGovernorComputePoolCloseRejectsQueries(t *testing.T) {
	governor, err := hatSql.NewNamespaceQueryGovernor(
		hatSql.NamespaceResourceLimits{ComputeWorkers: 1},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := governor.Close(); err != nil {
		t.Fatalf("close governor: %v", err)
	}
	_, err = governor.Execute(context.Background(), "default", "FROM VALUES (1) AS item(value) SELECT value", nil, nil, hatSql.QueryOptions{})
	if !errors.Is(err, hatSql.ErrNamespaceQueryGovernorClosed) {
		t.Fatalf("execute after close error = %v, want %v", err, hatSql.ErrNamespaceQueryGovernorClosed)
	}
}

func TestNamespaceQueryGovernorRejectsInvalidComputePoolLimits(t *testing.T) {
	for name, limits := range map[string]hatSql.NamespaceResourceLimits{
		"negative workers": {ComputeWorkers: -1},
		"negative queue":   {ComputeQueueCapacity: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatSql.NewNamespaceQueryGovernor(limits, nil); err == nil {
				t.Fatal("invalid compute limits were accepted")
			}
		})
	}
}
