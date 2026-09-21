package hatSql_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMZ047SQLQueryManagerRoutesNamedComputeCluster(t *testing.T) {
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeClusters: map[string]hatSql.SQLComputeClusterOptions{
			"analytics": {Workers: 1, QueueCapacity: 8},
		},
	})
	defer manager.Close()
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{
		ComputeCluster: "analytics",
	})
	if err != nil {
		t.Fatalf("named cluster Execute() error = %v", err)
	}
	clusters := manager.ComputeClusters()
	if len(clusters) != 1 || clusters[0] != "analytics" {
		t.Fatalf("ComputeClusters() = %#v, want [analytics]", clusters)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1) {
		t.Fatalf("named cluster result = %#v", result.Rows)
	}
	if _, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("empty compute cluster hint changed the direct default: %v", err)
	}
	if _, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{
		ComputeCluster: "missing",
	}); !errors.Is(err, hatSql.ErrSQLQueryManagerComputeClusterNotFound) {
		t.Fatalf("missing cluster error = %v, want %v", err, hatSql.ErrSQLQueryManagerComputeClusterNotFound)
	}
}

func TestMZ047SQLQueryManagerValidatesNamedComputeClusters(t *testing.T) {
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeClusters: map[string]hatSql.SQLComputeClusterOptions{
			"analytics": {Workers: 0},
		},
	})
	defer manager.Close()
	_, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", nil, nil, hatSql.QueryOptions{
		ComputeCluster: "analytics",
	})
	if !errors.Is(err, hatSql.ErrSQLQueryManagerComputeClusterInvalid) {
		t.Fatalf("invalid named cluster error = %v, want %v", err, hatSql.ErrSQLQueryManagerComputeClusterInvalid)
	}
}

func TestMZ047SQLQueryManagerClosesNamedComputeCluster(t *testing.T) {
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeClusters: map[string]hatSql.SQLComputeClusterOptions{
			"analytics": {Workers: 1},
		},
	})
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", nil, nil, hatSql.QueryOptions{
		ComputeCluster: "analytics",
	})
	if !errors.Is(err, hatSql.ErrSQLQueryManagerClosed) {
		t.Fatalf("closed named cluster error = %v, want %v", err, hatSql.ErrSQLQueryManagerClosed)
	}
}
