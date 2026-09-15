package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCH036SQLAggregateStateAndMerge(t *testing.T) {
	stateResult, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES ('north', 2), ('north', NULL), ('south', 5), ('north', 3)
AS events(region, amount)
SELECT events.region, SUM_STATE(events.amount) AS amount_state
GROUP BY events.region`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("SUM_STATE query error = %v", err)
	}
	if len(stateResult.Rows) != 2 {
		t.Fatalf("SUM_STATE rows = %#v, want two groups", stateResult.Rows)
	}
	states := make([]interface{}, 0, len(stateResult.Rows))
	for _, row := range stateResult.Rows {
		state, ok := row["amount_state"].([]byte)
		if !ok || len(state) == 0 {
			t.Fatalf("SUM_STATE value = %#v, want non-empty []byte", row["amount_state"])
		}
		states = append(states, state)
	}

	mergeResult, err := ExecuteSQLQueryParameters(context.Background(), `
FROM VALUES ($1), ($2) AS partial(state)
SELECT SUM_MERGE(partial.state) AS amount`, nil, states, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("SUM_MERGE query error = %v", err)
	}
	want := []SQLRow{{"amount": float64(10)}}
	if !reflect.DeepEqual(mergeResult.Rows, want) {
		t.Fatalf("SUM_MERGE rows = %#v, want %#v", mergeResult.Rows, want)
	}
}

func TestCH036SQLAggregateStateRejectsWrongFunctionAndMalformedState(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES ('not-a-state') AS partial(state)
SELECT SUM_MERGE(partial.state) AS amount`, nil, SQLQueryOptions{})
	if err == nil {
		t.Fatalf("malformed SUM_MERGE result = %#v, want an error", result.Rows)
	}

	result, err = ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1) AS values(value)
SELECT SUM_STATE(values.value, values.value) AS state`, nil, SQLQueryOptions{})
	if err == nil {
		t.Fatalf("wrong-arity SUM_STATE result = %#v, want an error", result.Rows)
	}

	countState, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1) AS events(value)
SELECT COUNT_STATE(events.value) AS state`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("COUNT_STATE query error = %v", err)
	}
	result, err = ExecuteSQLQueryParameters(context.Background(), `
FROM VALUES ($1) AS partial(state)
SELECT SUM_MERGE(partial.state) AS amount`, nil,
		[]interface{}{countState.Rows[0]["state"]}, SQLQueryOptions{})
	if err == nil {
		t.Fatalf("wrong-kind SUM_MERGE result = %#v, want an error", result.Rows)
	}
}

func TestCH036SQLAggregateStateAllKindsAndFilters(t *testing.T) {
	type aggregateCase struct {
		stateName string
		mergeName string
		want      interface{}
	}
	cases := []aggregateCase{
		{stateName: "COUNT_STATE", mergeName: "COUNT_MERGE", want: int64(2)},
		{stateName: "SUM_STATE", mergeName: "SUM_MERGE", want: float64(7)},
		{stateName: "AVG_STATE", mergeName: "AVG_MERGE", want: float64(3.5)},
		{stateName: "MIN_STATE", mergeName: "MIN_MERGE", want: float64(2)},
		{stateName: "MAX_STATE", mergeName: "MAX_MERGE", want: float64(5)},
	}
	for _, testCase := range cases {
		t.Run(testCase.stateName, func(t *testing.T) {
			makeState := func(values string) []byte {
				result, err := ExecuteSQLQueryContext(context.Background(),
					"FROM VALUES "+values+" AS events(amount) SELECT "+testCase.stateName+"(events.amount) AS state",
					nil, SQLQueryOptions{})
				if err != nil {
					t.Fatalf("state query error = %v", err)
				}
				if len(result.Rows) != 1 {
					t.Fatalf("state rows = %d, want 1", len(result.Rows))
				}
				state, ok := result.Rows[0]["state"].([]byte)
				if !ok || len(state) == 0 {
					t.Fatalf("state value = %#v, want non-empty []byte", result.Rows[0]["state"])
				}
				return state
			}

			left := makeState("(2), (NULL)")
			right := makeState("(5)")
			merged, err := ExecuteSQLQueryParameters(context.Background(),
				"FROM VALUES ($1), ($2) AS partial(state) SELECT "+testCase.mergeName+"(partial.state) AS result",
				nil, []interface{}{left, right}, SQLQueryOptions{})
			if err != nil {
				t.Fatalf("merge query error = %v", err)
			}
			if got := merged.Rows[0]["result"]; !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("merged result = %#v, want %#v", got, testCase.want)
			}
		})
	}

	countStar, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (NULL), (2), (5) AS events(amount)
SELECT COUNT_STATE(*) AS state`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("COUNT_STATE(*) query error = %v", err)
	}
	decoded, err := decodeSQLAggregateState(countStar.Rows[0]["state"].([]byte))
	if err != nil {
		t.Fatalf("COUNT_STATE(*) decode error = %v", err)
	}
	if got := decoded.result(false); got != int64(3) {
		t.Fatalf("COUNT_STATE(*) = %#v, want 3", got)
	}

	filtered, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1, true), (2, false), (5, true) AS events(amount, keep)
SELECT SUM_STATE(events.amount) FILTER (WHERE events.keep) AS state`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("filtered state query error = %v", err)
	}
	filteredState, err := decodeSQLAggregateState(filtered.Rows[0]["state"].([]byte))
	if err != nil {
		t.Fatalf("filtered state decode error = %v", err)
	}
	if got := filteredState.result(false); got != float64(6) {
		t.Fatalf("filtered SUM_STATE = %#v, want 6", got)
	}
}

func TestCH036SQLAggregateStateStrictEnvelope(t *testing.T) {
	valid := encodeSQLAggregateState(sqlAggregateStateAccumulator{
		kind:  sqlAggregateStateSum,
		count: 1,
		sum:   2,
		seen:  true,
	})
	seenWithoutCount := encodeSQLAggregateState(sqlAggregateStateAccumulator{
		kind: sqlAggregateStateSum,
		seen: true,
	})
	malformed := [][]byte{
		valid[:len(valid)-1],
		append(append([]byte(nil), valid...), 0),
		append([]byte("BAD!"), valid[4:]...),
		append([]byte{valid[0], valid[1], valid[2], valid[3], 2}, valid[5:]...),
		seenWithoutCount,
	}
	for index, state := range malformed {
		if _, err := decodeSQLAggregateState(state); err == nil {
			t.Errorf("malformed state %d decoded successfully", index)
		}
	}
}
