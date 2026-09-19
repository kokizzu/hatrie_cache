package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCHU44SQLAggregateStateAndMergeIfCombinators(t *testing.T) {
	state, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1, true), (2, false), (5, true) AS events(amount, keep)
SELECT SUM_STATE_IF(events.amount, events.keep) AS state`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("SUM_STATE_IF query error = %v", err)
	}
	serialized, ok := state.Rows[0]["state"].([]byte)
	if !ok || len(serialized) == 0 {
		t.Fatalf("SUM_STATE_IF value = %#v, want serialized state", state.Rows[0]["state"])
	}

	count, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1, true), (2, false), (5, true) AS events(amount, keep)
SELECT COUNT_STATE_IF(events.amount, events.keep) AS state`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("COUNT_STATE_IF query error = %v", err)
	}
	countState, ok := count.Rows[0]["state"].([]byte)
	if !ok || len(countState) == 0 {
		t.Fatalf("COUNT_STATE_IF value = %#v, want serialized state", count.Rows[0]["state"])
	}

	merged, err := ExecuteSQLQueryParameters(context.Background(), `
FROM VALUES ($1, true), ($2, false) AS partial(state, keep)
SELECT SUM_MERGE_IF(partial.state, partial.keep) AS amount`, nil,
		[]interface{}{serialized, countState}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("SUM_MERGE_IF query error = %v", err)
	}
	if want := []SQLRow{{"amount": float64(6)}}; !reflect.DeepEqual(merged.Rows, want) {
		t.Fatalf("SUM_MERGE_IF rows = %#v, want %#v", merged.Rows, want)
	}
}

func TestCHU44SQLAggregateStateIfCombinatorsValidateConditionArity(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1, true) AS events(amount, keep)
SELECT SUM_STATE_IF(events.amount) AS state`, nil, SQLQueryOptions{})
	if err == nil {
		t.Fatalf("invalid SUM_STATE_IF result = %#v, want an arity error", result.Rows)
	}

	result, err = ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1, true) AS events(amount, keep)
SELECT SUM_MERGE_IF(events.amount, events.keep, events.keep) AS state`, nil, SQLQueryOptions{})
	if err == nil {
		t.Fatalf("invalid SUM_MERGE_IF result = %#v, want an arity error", result.Rows)
	}

	result, err = ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1, true) AS events(amount, keep)
SELECT SUM_STATE_IF(events.amount, *) AS state`, nil, SQLQueryOptions{})
	if err == nil {
		t.Fatalf("invalid SUM_STATE_IF star condition result = %#v, want a diagnostic", result.Rows)
	}
}

func TestCHU44SQLAggregateStateIfCombinatorCombinesWithFilter(t *testing.T) {
	state, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1, true), (2, true), (5, false) AS events(amount, keep)
SELECT SUM_STATE_IF(events.amount, events.keep) FILTER (WHERE events.amount > 1) AS state`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("SUM_STATE_IF with FILTER query error = %v", err)
	}
	serialized, ok := state.Rows[0]["state"].([]byte)
	if !ok || len(serialized) == 0 {
		t.Fatalf("SUM_STATE_IF with FILTER value = %#v, want serialized state", state.Rows[0]["state"])
	}

	merged, err := ExecuteSQLQueryParameters(context.Background(), `
FROM VALUES ($1) AS partial(state)
SELECT SUM_MERGE(partial.state) AS amount`, nil, []interface{}{serialized}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("SUM_MERGE filtered state query error = %v", err)
	}
	if want := []SQLRow{{"amount": float64(2)}}; !reflect.DeepEqual(merged.Rows, want) {
		t.Fatalf("SUM_MERGE filtered state rows = %#v, want %#v", merged.Rows, want)
	}
}

func TestCHU44SQLArgExtremeStateIfCombinators(t *testing.T) {
	state, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES ('alpha', 10, true), ('beta', 20, false), ('gamma', 30, true) AS events(payload, score, keep)
SELECT ARGMAX_STATE_IF(events.payload, events.score, events.keep) AS state`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ARGMAX_STATE_IF query error = %v", err)
	}
	serialized, ok := state.Rows[0]["state"].([]byte)
	if !ok || len(serialized) == 0 {
		t.Fatalf("ARGMAX_STATE_IF value = %#v, want serialized state", state.Rows[0]["state"])
	}

	merged, err := ExecuteSQLQueryParameters(context.Background(), `
FROM VALUES ($1, true), ($2, false) AS partial(state, keep)
SELECT ARGMAX_MERGE_IF(partial.state, partial.keep) AS payload`, nil,
		[]interface{}{serialized, serialized}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ARGMAX_MERGE_IF query error = %v", err)
	}
	if want := []SQLRow{{"payload": "gamma"}}; !reflect.DeepEqual(merged.Rows, want) {
		t.Fatalf("ARGMAX_MERGE_IF rows = %#v, want %#v", merged.Rows, want)
	}
}
