package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type ch035RemoteShardResolver struct {
	rows           []Row
	pruneAvailable bool
	include        bool
	pruneErr       error
	predicateCalls int
	queryCalls     int
	lastName       string
	lastKey        string
	lastPredicates []SQLPartitionPredicate
}

func (resolver *ch035RemoteShardResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	resolver.queryCalls++
	return resolver.rows, nil
}

func (resolver *ch035RemoteShardResolver) ShouldQuerySQLDistributedShard(name, key string, predicates []SQLPartitionPredicate) (bool, bool, error) {
	resolver.predicateCalls++
	resolver.lastName = name
	resolver.lastKey = key
	resolver.lastPredicates = append([]SQLPartitionPredicate(nil), predicates...)
	if resolver.pruneErr != nil {
		return true, true, resolver.pruneErr
	}
	if !resolver.pruneAvailable {
		return true, false, nil
	}
	return resolver.include, true, nil
}

func TestCH035RemoteShardPruningSkipsNonMatchingShardsAndBindsParameters(t *testing.T) {
	matching := &ch035RemoteShardResolver{
		rows:           []Row{{"id": int64(1), "region": "apac"}},
		pruneAvailable: true,
		include:        true,
	}
	prunedOne := &ch035RemoteShardResolver{
		rows:           []Row{{"id": int64(2), "region": "eu"}},
		pruneAvailable: true,
		include:        false,
	}
	prunedTwo := &ch035RemoteShardResolver{
		rows:           []Row{{"id": int64(3), "region": "eu"}},
		pruneAvailable: true,
		include:        false,
	}
	shards := []SQLDistributedQueryShard{
		{ID: "apac", Resolver: matching},
		{ID: "eu-1", Resolver: prunedOne},
		{ID: "eu-2", Resolver: prunedTwo},
	}

	result, err := ExecuteSQLDistributedQuery(
		context.Background(),
		"FROM CACHE('users') SELECT id, region WHERE region = $1",
		shards,
		[]interface{}{"apac"},
		SQLQueryOptions{},
		SQLDistributedQueryOptions{MaxConcurrency: 3},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLDistributedQuery() error = %v", err)
	}
	if want := []Row{{"id": int64(1), "region": "apac"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
	if matching.queryCalls != 1 || prunedOne.queryCalls != 0 || prunedTwo.queryCalls != 0 {
		t.Fatalf("query calls = matching %d, pruned %d/%d; want 1, 0/0", matching.queryCalls, prunedOne.queryCalls, prunedTwo.queryCalls)
	}
	for name, resolver := range map[string]*ch035RemoteShardResolver{"matching": matching, "pruned-one": prunedOne, "pruned-two": prunedTwo} {
		if resolver.predicateCalls != 1 {
			t.Fatalf("%s predicate calls = %d, want 1", name, resolver.predicateCalls)
		}
		if resolver.lastName != "CACHE" || resolver.lastKey != "users" {
			t.Fatalf("%s source = %q/%q, want CACHE/users", name, resolver.lastName, resolver.lastKey)
		}
		wantPredicate := []SQLPartitionPredicate{{Field: "region", Operator: "=", Values: []interface{}{"apac"}}}
		if !reflect.DeepEqual(resolver.lastPredicates, wantPredicate) {
			t.Fatalf("%s predicates = %#v, want %#v", name, resolver.lastPredicates, wantPredicate)
		}
	}
}

func TestCH035RemoteShardPruningUnavailableFallsBackToEveryShard(t *testing.T) {
	first := &ch035RemoteShardResolver{
		rows:           []Row{{"id": int64(1), "region": "apac"}},
		pruneAvailable: false,
	}
	second := &ch035RemoteShardResolver{
		rows:           []Row{{"id": int64(2), "region": "apac"}},
		pruneAvailable: false,
	}

	result, err := ExecuteSQLDistributedQuery(
		context.Background(),
		"FROM CACHE('users') SELECT id WHERE region = 'apac'",
		[]SQLDistributedQueryShard{{ID: "one", Resolver: first}, {ID: "two", Resolver: second}},
		nil,
		SQLQueryOptions{},
		SQLDistributedQueryOptions{MaxConcurrency: 2},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLDistributedQuery() error = %v", err)
	}
	if want := []Row{{"id": int64(1)}, {"id": int64(2)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
	if first.queryCalls != 1 || second.queryCalls != 1 {
		t.Fatalf("query calls = %d/%d, want 1/1", first.queryCalls, second.queryCalls)
	}
}

func TestCH035RemoteShardPruningAllShardsPreservesAggregateShape(t *testing.T) {
	first := &ch035RemoteShardResolver{pruneAvailable: true, include: false}
	second := &ch035RemoteShardResolver{pruneAvailable: true, include: false}

	result, err := ExecuteSQLDistributedQuery(
		context.Background(),
		"FROM CACHE('users') SELECT COUNT(*) AS total WHERE region = 'apac'",
		[]SQLDistributedQueryShard{{ID: "one", Resolver: first}, {ID: "two", Resolver: second}},
		nil,
		SQLQueryOptions{},
		SQLDistributedQueryOptions{MaxConcurrency: 2},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLDistributedQuery() error = %v", err)
	}
	if want := []string{"total"}; !reflect.DeepEqual(result.Columns, want) {
		t.Fatalf("result.Columns = %#v, want %#v", result.Columns, want)
	}
	if want := []Row{{"total": int64(0)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
	if first.queryCalls != 0 || second.queryCalls != 0 {
		t.Fatalf("query calls = %d/%d, want 0/0", first.queryCalls, second.queryCalls)
	}
}

func TestCH035RemoteShardPruningSkipsWhenNoSafePredicateExists(t *testing.T) {
	resolver := &ch035RemoteShardResolver{rows: []Row{{"id": int64(1)}}, pruneAvailable: true, include: false}
	result, err := ExecuteSQLDistributedQuery(
		context.Background(),
		"FROM CACHE('users') SELECT id",
		[]SQLDistributedQueryShard{{ID: "one", Resolver: resolver}},
		nil,
		SQLQueryOptions{},
		SQLDistributedQueryOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLDistributedQuery() error = %v", err)
	}
	if want := []Row{{"id": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.predicateCalls != 0 || resolver.queryCalls != 1 {
		t.Fatalf("predicate/query calls = %d/%d, want 0/1", resolver.predicateCalls, resolver.queryCalls)
	}
}

func TestCH035RemoteShardPruningPropagatesMetadataError(t *testing.T) {
	wantErr := errors.New("metadata unavailable")
	resolver := &ch035RemoteShardResolver{pruneAvailable: true, pruneErr: wantErr}
	_, err := ExecuteSQLDistributedQuery(
		context.Background(),
		"FROM CACHE('users') SELECT id WHERE region = 'apac'",
		[]SQLDistributedQueryShard{{ID: "one", Resolver: resolver}},
		nil,
		SQLQueryOptions{},
		SQLDistributedQueryOptions{},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ExecuteSQLDistributedQuery() error = %v, want %v", err, wantErr)
	}
	if resolver.queryCalls != 0 {
		t.Fatalf("query calls = %d, want 0 after metadata error", resolver.queryCalls)
	}
}
