package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCH037SQLArgExtremeStateAndMerge(t *testing.T) {
	type aggregateCase struct {
		stateName string
		mergeName string
		want      interface{}
	}
	cases := []aggregateCase{
		{stateName: "ARGMAX_STATE", mergeName: "ARGMAX_MERGE", want: "gamma"},
		{stateName: "ARGMIN_STATE", mergeName: "ARGMIN_MERGE", want: "alpha"},
	}
	for _, testCase := range cases {
		t.Run(testCase.stateName, func(t *testing.T) {
			makeState := func(values string) []byte {
				result, err := ExecuteSQLQueryContext(context.Background(),
					"FROM VALUES "+values+" AS events(payload, score) SELECT "+testCase.stateName+"(events.payload, events.score) AS state",
					nil, SQLQueryOptions{})
				if err != nil {
					t.Fatalf("state query error = %v", err)
				}
				state, ok := result.Rows[0]["state"].([]byte)
				if !ok || len(state) == 0 {
					t.Fatalf("state value = %#v, want non-empty []byte", result.Rows[0]["state"])
				}
				return state
			}

			left := makeState("('alpha', 10), ('beta', 20)")
			right := makeState("('gamma', 30), (NULL, 40), ('ignored', NULL)")
			merged, err := ExecuteSQLQueryParameters(context.Background(),
				"FROM VALUES ($1), ($2) AS partial(state) SELECT "+testCase.mergeName+"(partial.state) AS value",
				nil, []interface{}{left, right}, SQLQueryOptions{})
			if err != nil {
				t.Fatalf("merge query error = %v", err)
			}
			if got := merged.Rows[0]["value"]; !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("merged value = %#v, want %#v", got, testCase.want)
			}
		})
	}
}

func TestCH037SQLArgExtremeStateGroupedAndTieSafe(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES ('north', 'first', 10), ('north', 'same-score', 10), ('south', NULL, 1), ('south', 'valid', 2)
AS events(region, payload, score)
SELECT events.region, ARGMAX_STATE(events.payload, events.score) AS state
GROUP BY events.region`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("grouped state query error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("grouped state rows = %d, want 2", len(result.Rows))
	}
	states := make([]interface{}, 0, len(result.Rows))
	for _, row := range result.Rows {
		state, ok := row["state"].([]byte)
		if !ok || len(state) == 0 {
			t.Fatalf("grouped state = %#v, want non-empty []byte", row["state"])
		}
		states = append(states, state)
	}
	merged, err := ExecuteSQLQueryParameters(context.Background(), `
FROM VALUES ($1), ($2) AS partial(state)
SELECT ARGMAX_MERGE(partial.state) AS value`, nil, states, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("grouped merge error = %v", err)
	}
	if got := merged.Rows[0]["value"]; got != "first" && got != "valid" {
		t.Fatalf("tie-safe merged value = %#v, want first-seen winner", got)
	}
}

func TestCH037SQLArgExtremeStateRejectsMalformedAndWrongKind(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES ('not-a-state') AS partial(state)
SELECT ARGMAX_MERGE(partial.state) AS value`, nil, SQLQueryOptions{})
	if err == nil {
		t.Fatalf("malformed ARGMAX_MERGE result = %#v, want an error", result.Rows)
	}

	result, err = ExecuteSQLQueryContext(context.Background(), `
FROM VALUES ('payload', 1) AS events(payload, score)
SELECT ARGMAX_STATE(events.payload) AS state`, nil, SQLQueryOptions{})
	if err == nil {
		t.Fatalf("wrong-arity ARGMAX_STATE result = %#v, want an error", result.Rows)
	}

	countState, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1) AS events(value)
SELECT COUNT_STATE(events.value) AS state`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("COUNT_STATE query error = %v", err)
	}
	result, err = ExecuteSQLQueryParameters(context.Background(), `
FROM VALUES ($1) AS partial(state)
SELECT ARGMAX_MERGE(partial.state) AS value`, nil,
		[]interface{}{countState.Rows[0]["state"]}, SQLQueryOptions{})
	if err == nil {
		t.Fatalf("wrong-kind ARGMAX_MERGE result = %#v, want an error", result.Rows)
	}
}
