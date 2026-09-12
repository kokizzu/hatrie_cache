package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLPartialAggregateStateIsPublicAPI(t *testing.T) {
	state := hatSql.SQLPartialAggregateState{Groups: []hatSql.SQLPartialAggregateGroup{{
		Key:    "north",
		Value:  "North",
		Count:  2,
		Sum:    7,
		HasSum: true,
	}}}
	wire, err := hatSql.EncodeSQLPartialAggregateState(state)
	if err != nil {
		t.Fatalf("encode public state: %v", err)
	}
	decoded, err := hatSql.DecodeSQLPartialAggregateState(wire)
	if err != nil {
		t.Fatalf("decode public state: %v", err)
	}
	if err := hatSql.MergeSQLPartialAggregateState(&decoded, state); err != nil {
		t.Fatalf("merge public state: %v", err)
	}
	if len(decoded.Groups) != 1 || decoded.Groups[0].Count != 4 || decoded.Groups[0].Sum != 14 {
		t.Fatalf("decoded public state = %#v", decoded)
	}
}
