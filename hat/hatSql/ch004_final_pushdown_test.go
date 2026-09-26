package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type ch004FinalPushdownResolver struct {
	rawRows     []SQLRow
	finalRows   []SQLRow
	declineFinal bool
	sourceCalls int
	finalCalls  int
}

func (resolver *ch004FinalPushdownResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name != "CACHE" || key != "events" {
		return nil, nil
	}
	resolver.sourceCalls++
	return resolver.rawRows, nil
}

func (resolver *ch004FinalPushdownResolver) ResolveSQLFinalSource(name, key string, options SQLFinalOptions) ([]Row, bool, error) {
	if name != "CACHE" || key != "events" {
		return nil, false, nil
	}
	if options.Mode != SQLFinalReplacing || options.Key == nil || options.Version == nil || options.Sign != nil {
		return nil, false, errors.New("unexpected FINAL options")
	}
	resolver.finalCalls++
	if resolver.declineFinal {
		return nil, false, nil
	}
	return resolver.finalRows, true, nil
}

func TestCH004FinalProviderPushdownUsesReconciledRows(t *testing.T) {
	resolver := &ch004FinalPushdownResolver{
		rawRows: []SQLRow{
			{"id": "a", "version": uint64(1), "value": "old"},
			{"id": "a", "version": uint64(2), "value": "new"},
			{"id": "b", "version": uint64(1), "value": "b"},
		},
		finalRows: []SQLRow{
			{"id": "a", "version": uint64(2), "value": "new"},
			{"id": "b", "version": uint64(1), "value": "b"},
		},
	}
	result, err := ExecuteSQLQueryParameters(context.Background(),
		"FROM CACHE('events') AS event FINAL SELECT event.id, event.value ORDER BY event.id",
		resolver, nil, SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(ch004ReplacingFinalOptions)})
	if err != nil {
		t.Fatalf("FINAL query: %v", err)
	}
	want := []SQLRow{
		{"id": "a", "value": "new"},
		{"id": "b", "value": "b"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("FINAL rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.finalCalls != 1 {
		t.Fatalf("physical FINAL calls = %d, want 1", resolver.finalCalls)
	}
	if resolver.sourceCalls != 0 {
		t.Fatalf("materialized source calls = %d, want 0", resolver.sourceCalls)
	}
}

func TestCH004FinalProviderPushdownCacheSeparation(t *testing.T) {
	newResolver := func() *ch004FinalPushdownResolver {
		return &ch004FinalPushdownResolver{
			rawRows: []SQLRow{
				{"id": "a", "version": uint64(1), "value": "old"},
				{"id": "a", "version": uint64(2), "value": "new"},
				{"id": "b", "version": uint64(1), "value": "b"},
			},
			finalRows: []SQLRow{
				{"id": "a", "version": uint64(2), "value": "new"},
				{"id": "b", "version": uint64(1), "value": "b"},
			},
		}
	}
	options := SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(ch004ReplacingFinalOptions)}

	finalResolver := newResolver()
	result, err := ExecuteSQLQueryParameters(context.Background(),
		"FROM CACHE('events') AS event FINAL JOIN CACHE('events') AS other FINAL ON event.id = other.id SELECT event.id, other.value AS other_value ORDER BY event.id",
		finalResolver, nil, options)
	if err != nil {
		t.Fatalf("repeated FINAL query: %v", err)
	}
	want := []SQLRow{
		{"id": "a", "other_value": "new"},
		{"id": "b", "other_value": "b"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("repeated FINAL rows = %#v, want %#v", result.Rows, want)
	}
	if finalResolver.finalCalls != 1 || finalResolver.sourceCalls != 0 {
		t.Fatalf("repeated FINAL resolver calls = final %d, source %d, want final 1, source 0", finalResolver.finalCalls, finalResolver.sourceCalls)
	}

	rawResolver := newResolver()
	result, err = ExecuteSQLQueryParameters(context.Background(),
		"FROM CACHE('events') AS event FINAL JOIN CACHE('events') AS raw ON event.id = raw.id SELECT event.id, raw.value AS raw_value",
		rawResolver, nil, options)
	if err != nil {
		t.Fatalf("mixed FINAL query: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("mixed FINAL rows = %d, want 3", len(result.Rows))
	}
	if rawResolver.finalCalls != 1 || rawResolver.sourceCalls != 1 {
		t.Fatalf("mixed FINAL resolver calls = final %d, source %d, want final 1, source 1", rawResolver.finalCalls, rawResolver.sourceCalls)
	}
}

func TestCH004FinalProviderPushdownFallsBackWhenUnavailable(t *testing.T) {
	resolver := &ch004FinalPushdownResolver{
		rawRows: []SQLRow{
			{"id": "a", "version": uint64(1), "value": "old"},
			{"id": "a", "version": uint64(2), "value": "new"},
		},
		declineFinal: true,
	}
	result, err := ExecuteSQLQueryParameters(context.Background(),
		"FROM CACHE('events') AS event FINAL SELECT event.id, event.value",
		resolver, nil, SQLQueryOptions{FinalSourceOptions: ch004FinalSourceOptionsResolver(ch004ReplacingFinalOptions)})
	if err != nil {
		t.Fatalf("fallback FINAL query: %v", err)
	}
	want := []SQLRow{{"id": "a", "value": "new"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("fallback FINAL rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.finalCalls != 1 || resolver.sourceCalls != 1 {
		t.Fatalf("fallback resolver calls = final %d, source %d, want final 1, source 1", resolver.finalCalls, resolver.sourceCalls)
	}
}
