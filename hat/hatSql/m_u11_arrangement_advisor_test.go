//go:build mu11

package hatSql

import (
	"reflect"
	"testing"
)

func TestTypedTableAggregateArrangementAdvisorIsDeterministic(t *testing.T) {
	definition := TypedTableAggregateDefinition{
		GroupBy:  []string{"region"},
		SumField: "amount",
	}
	options := TypedTableArrangementAdvisorOptions{
		AggregateStateBytesPerRow: 160,
		JoinStateBytesPerRow:      128,
		FixedArrangementBytes:     256,
		ReplayBytesPerChange:      64,
	}
	catalog := []TypedTableAggregateArrangementInfo{
		{
			TableName:      "orders",
			Definition:     definition,
			References:     1,
			Checkpoint:     3,
			SourceSequence: 8,
			Stale:          true,
		},
		{
			TableName:      "orders",
			Definition:     definition,
			References:     2,
			Shared:         true,
			Checkpoint:     8,
			SourceSequence: 8,
		},
		{
			TableName:  "other",
			Definition: definition,
		},
	}
	request := TypedTableAggregateArrangementRequest{
		TableName:          "orders",
		Definition:         definition,
		EstimatedStateRows: 100,
	}

	got := AdviseTypedTableAggregateArrangement(catalog, request, options)
	reversed := append([]TypedTableAggregateArrangementInfo(nil), catalog...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	if reverseGot := AdviseTypedTableAggregateArrangement(reversed, request, options); !reflect.DeepEqual(got, reverseGot) {
		t.Fatalf("advisor changed with catalog order:\nfirst=%#v\nsecond=%#v", got, reverseGot)
	}
	if got.Action != TypedTableArrangementAdvisorReuse {
		t.Fatalf("Action = %q, want %q", got.Action, TypedTableArrangementAdvisorReuse)
	}
	if len(got.Candidates) != 2 {
		t.Fatalf("candidate count = %d, want 2", len(got.Candidates))
	}
	if got.Candidates[0].Stale || got.Candidates[0].Action != TypedTableArrangementAdvisorReuse {
		t.Fatalf("fresh candidate = %#v, want first and immediately reusable", got.Candidates[0])
	}
	if got.Candidates[0].Memory != (TypedTableArrangementMemoryEstimate{}) {
		t.Fatalf("fresh candidate memory = %#v, want zero additional memory", got.Candidates[0].Memory)
	}
	if !got.Candidates[1].Stale || got.Candidates[1].Action != TypedTableArrangementAdvisorHydrateThenReuse {
		t.Fatalf("stale candidate = %#v, want hydrate-then-reuse", got.Candidates[1])
	}
	if want := uint64(5 * 64); got.Candidates[1].Memory.TransientBytes != want {
		t.Fatalf("stale transient bytes = %d, want %d", got.Candidates[1].Memory.TransientBytes, want)
	}
}

func TestTypedTableArrangementAdvisorCreatesWhenNoExactCandidateExists(t *testing.T) {
	definition := TypedTableAggregateDefinition{GroupBy: []string{"region"}}
	options := TypedTableArrangementAdvisorOptions{
		AggregateStateBytesPerRow: 11,
		FixedArrangementBytes:     7,
		ReplayBytesPerChange:      3,
	}
	got := AdviseTypedTableAggregateArrangement(nil, TypedTableAggregateArrangementRequest{
		TableName:          "orders",
		Definition:         definition,
		EstimatedStateRows: 9,
	}, options)
	if got.Action != TypedTableArrangementAdvisorCreate {
		t.Fatalf("Action = %q, want %q", got.Action, TypedTableArrangementAdvisorCreate)
	}
	if len(got.Candidates) != 0 {
		t.Fatalf("candidate count = %d, want 0", len(got.Candidates))
	}
	if got.Memory.PersistentBytes != 106 || got.Memory.TotalBytes != 106 {
		t.Fatalf("create memory = %#v, want persistent/total 106", got.Memory)
	}
}

func TestTypedTableJoinArrangementAdvisorHandlesStaleInputsAndOrientation(t *testing.T) {
	definition := TypedTableJoinDefinition{LeftField: "customer_id", RightField: "id"}
	options := TypedTableArrangementAdvisorOptions{
		JoinStateBytesPerRow:  12,
		FixedArrangementBytes: 100,
		ReplayBytesPerChange:  7,
	}
	request := TypedTableJoinArrangementRequest{
		LeftTableName:      "orders",
		RightTableName:     "customers",
		Definition:         definition,
		EstimatedStateRows: 5,
	}
	fresh := AdviseTypedTableJoinArrangement([]TypedTableJoinArrangementInfo{{
		LeftTableName:       "orders",
		RightTableName:      "customers",
		Definition:          definition,
		LeftCheckpoint:      10,
		LeftSourceSequence:  10,
		RightCheckpoint:     12,
		RightSourceSequence: 12,
	}}, request, options)
	if fresh.Action != TypedTableArrangementAdvisorReuse || len(fresh.Candidates) != 1 {
		t.Fatalf("fresh join advice = %#v, want one reusable candidate", fresh)
	}

	stale := AdviseTypedTableJoinArrangement([]TypedTableJoinArrangementInfo{{
		LeftTableName:       "orders",
		RightTableName:      "customers",
		Definition:          definition,
		LeftCheckpoint:      8,
		LeftSourceSequence:  10,
		RightCheckpoint:     9,
		RightSourceSequence: 12,
		Stale:               true,
	}}, request, options)
	if stale.Action != TypedTableArrangementAdvisorHydrateThenReuse {
		t.Fatalf("stale join action = %q, want %q", stale.Action, TypedTableArrangementAdvisorHydrateThenReuse)
	}
	if want := uint64((2 + 3) * 7); stale.Memory.TransientBytes != want {
		t.Fatalf("stale join transient bytes = %d, want %d", stale.Memory.TransientBytes, want)
	}

	wrongOrientation := AdviseTypedTableJoinArrangement([]TypedTableJoinArrangementInfo{{
		LeftTableName:  "customers",
		RightTableName: "orders",
		Definition:     definition,
	}}, request, options)
	if wrongOrientation.Action != TypedTableArrangementAdvisorCreate || len(wrongOrientation.Candidates) != 0 {
		t.Fatalf("wrong orientation advice = %#v, want create with no candidate", wrongOrientation)
	}
}
