//go:build mu45

package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestSelectTypedTableJoinArrangementChoosesLowestIncrementalMemory(t *testing.T) {
	options := TypedTableArrangementAdvisorOptions{
		JoinStateBytesPerRow:  10,
		FixedArrangementBytes: 4,
		ReplayBytesPerChange:  3,
	}
	request := TypedTableJoinArrangementSelectionRequest{
		Alternatives: []TypedTableJoinArrangementRequest{
			{
				LeftTableName:      "orders",
				RightTableName:     "customers",
				Definition:         TypedTableJoinDefinition{LeftField: "customer_id", RightField: "id"},
				EstimatedStateRows: 2,
			},
			{
				LeftTableName:      "orders",
				RightTableName:     "customers",
				Definition:         TypedTableJoinDefinition{LeftField: "account_id", RightField: "id"},
				EstimatedStateRows: 2,
			},
			{
				LeftTableName:      "orders",
				RightTableName:     "customers",
				Definition:         TypedTableJoinDefinition{LeftField: "region_id", RightField: "id"},
				EstimatedStateRows: 10,
			},
		},
	}
	catalog := []TypedTableJoinArrangementInfo{
		{
			LeftTableName:       "orders",
			RightTableName:      "customers",
			Definition:          request.Alternatives[0].Definition,
			LeftCheckpoint:      0,
			LeftSourceSequence:  5,
			RightCheckpoint:     0,
			RightSourceSequence: 5,
		},
	}

	got, err := SelectTypedTableJoinArrangement(catalog, request, options)
	if err != nil {
		t.Fatalf("SelectTypedTableJoinArrangement() error = %v", err)
	}
	if got.Selected.Request.Definition.LeftField != "account_id" {
		t.Fatalf("selected left field = %q, want account_id", got.Selected.Request.Definition.LeftField)
	}
	if got.Selected.Advice.Action != TypedTableArrangementAdvisorCreate {
		t.Fatalf("selected action = %q, want create", got.Selected.Advice.Action)
	}
	if got.Selected.Advice.Memory.TotalBytes != 24 {
		t.Fatalf("selected total bytes = %d, want 24", got.Selected.Advice.Memory.TotalBytes)
	}
	if len(got.Candidates) != 3 {
		t.Fatalf("candidate count = %d, want 3", len(got.Candidates))
	}
	if got.Candidates[0].Request.Definition.LeftField != "account_id" {
		t.Fatalf("best candidate = %q, want account_id", got.Candidates[0].Request.Definition.LeftField)
	}
	if got.Candidates[1].Request.Definition.LeftField != "customer_id" || got.Candidates[2].Request.Definition.LeftField != "region_id" {
		t.Fatalf("candidate order = %#v, want account_id, customer_id, region_id", got.Candidates)
	}
}

func TestSelectTypedTableJoinArrangementIsDeterministicAndExplainable(t *testing.T) {
	definitionA := TypedTableJoinDefinition{LeftField: "customer_id", RightField: "id"}
	definitionB := TypedTableJoinDefinition{LeftField: "account_id", RightField: "id"}
	request := TypedTableJoinArrangementSelectionRequest{Alternatives: []TypedTableJoinArrangementRequest{
		{LeftTableName: "orders", RightTableName: "customers", Definition: definitionA, EstimatedStateRows: 4},
		{LeftTableName: "orders", RightTableName: "customers", Definition: definitionB, EstimatedStateRows: 4},
	}}
	catalog := []TypedTableJoinArrangementInfo{
		{
			LeftTableName:       "orders",
			RightTableName:      "customers",
			Definition:          definitionA,
			References:          1,
			LeftCheckpoint:      9,
			LeftSourceSequence:  9,
			RightCheckpoint:     9,
			RightSourceSequence: 9,
		},
		{
			LeftTableName:       "orders",
			RightTableName:      "customers",
			Definition:          definitionB,
			References:          5,
			LeftCheckpoint:      9,
			LeftSourceSequence:  9,
			RightCheckpoint:     9,
			RightSourceSequence: 9,
		},
	}

	first, err := SelectTypedTableJoinArrangement(catalog, request, TypedTableArrangementAdvisorOptions{})
	if err != nil {
		t.Fatalf("first selection error = %v", err)
	}
	reversedCatalog := append([]TypedTableJoinArrangementInfo(nil), catalog...)
	reversedCatalog[0], reversedCatalog[1] = reversedCatalog[1], reversedCatalog[0]
	reversedRequest := TypedTableJoinArrangementSelectionRequest{Alternatives: append([]TypedTableJoinArrangementRequest(nil), request.Alternatives...)}
	reversedRequest.Alternatives[0], reversedRequest.Alternatives[1] = reversedRequest.Alternatives[1], reversedRequest.Alternatives[0]
	second, err := SelectTypedTableJoinArrangement(reversedCatalog, reversedRequest, TypedTableArrangementAdvisorOptions{})
	if err != nil {
		t.Fatalf("reversed selection error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("selection changed with input order:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if first.Selected.Request.Definition != definitionB {
		t.Fatalf("selected definition = %#v, want definitionB after sharing tie-breaks", first.Selected.Request.Definition)
	}
	if len(first.Selected.Advice.Candidates) != 1 || first.Selected.Advice.Candidates[0].References != 5 {
		t.Fatalf("selected advice = %#v, want exact candidate explanation", first.Selected.Advice)
	}
}

func TestSelectTypedTableJoinArrangementRejectsAmbiguousAlternatives(t *testing.T) {
	base := TypedTableJoinArrangementRequest{
		LeftTableName:  "orders",
		RightTableName: "customers",
		Definition:     TypedTableJoinDefinition{LeftField: "customer_id", RightField: "id"},
	}
	tests := []struct {
		name         string
		alternatives []TypedTableJoinArrangementRequest
		wantErr      error
	}{
		{name: "empty", alternatives: nil, wantErr: ErrTypedTableJoinArrangementSelectionNoCandidates},
		{
			name: "mixed orientation",
			alternatives: []TypedTableJoinArrangementRequest{
				base,
				{LeftTableName: "customers", RightTableName: "orders", Definition: base.Definition},
			},
			wantErr: ErrTypedTableJoinArrangementSelectionTableMismatch,
		},
		{
			name:         "duplicate definition",
			alternatives: []TypedTableJoinArrangementRequest{base, base},
			wantErr:      ErrTypedTableJoinArrangementSelectionDuplicate,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := SelectTypedTableJoinArrangement(nil, TypedTableJoinArrangementSelectionRequest{Alternatives: test.alternatives}, TypedTableArrangementAdvisorOptions{})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
		})
	}
}
