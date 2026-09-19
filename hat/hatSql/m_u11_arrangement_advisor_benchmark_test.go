//go:build mu11

package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkMU11AggregateArrangementAdvisor(b *testing.B) {
	definition := TypedTableAggregateDefinition{GroupBy: []string{"region"}, SumField: "amount"}
	catalog := make([]TypedTableAggregateArrangementInfo, 64)
	for index := range catalog {
		catalog[index] = TypedTableAggregateArrangementInfo{
			TableName:  "noise_" + strconv.Itoa(index),
			Definition: TypedTableAggregateDefinition{GroupBy: []string{"other"}},
		}
	}
	catalog[32] = TypedTableAggregateArrangementInfo{
		TableName:      "orders",
		Definition:     definition,
		References:     2,
		Shared:         true,
		Checkpoint:     100,
		SourceSequence: 100,
	}
	request := TypedTableAggregateArrangementRequest{
		TableName:          "orders",
		Definition:         definition,
		EstimatedStateRows: 1000,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		advice := AdviseTypedTableAggregateArrangement(catalog, request, TypedTableArrangementAdvisorOptions{})
		if advice.Action != TypedTableArrangementAdvisorReuse {
			b.Fatalf("Action = %q, want reuse", advice.Action)
		}
	}
}

func BenchmarkMU11JoinArrangementAdvisor(b *testing.B) {
	definition := TypedTableJoinDefinition{LeftField: "customer_id", RightField: "id"}
	catalog := make([]TypedTableJoinArrangementInfo, 64)
	for index := range catalog {
		catalog[index] = TypedTableJoinArrangementInfo{
			LeftTableName:  "left_" + strconv.Itoa(index),
			RightTableName: "right_" + strconv.Itoa(index),
			Definition:     TypedTableJoinDefinition{LeftField: "other", RightField: "key"},
		}
	}
	catalog[32] = TypedTableJoinArrangementInfo{
		LeftTableName:       "orders",
		RightTableName:      "customers",
		Definition:          definition,
		References:          2,
		Shared:              true,
		LeftCheckpoint:      100,
		LeftSourceSequence:  100,
		RightCheckpoint:     100,
		RightSourceSequence: 100,
	}
	request := TypedTableJoinArrangementRequest{
		LeftTableName:      "orders",
		RightTableName:     "customers",
		Definition:         definition,
		EstimatedStateRows: 1000,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		advice := AdviseTypedTableJoinArrangement(catalog, request, TypedTableArrangementAdvisorOptions{})
		if advice.Action != TypedTableArrangementAdvisorReuse {
			b.Fatalf("Action = %q, want reuse", advice.Action)
		}
	}
}

func BenchmarkMU11AggregateArrangementAdvisorCreate(b *testing.B) {
	request := TypedTableAggregateArrangementRequest{
		TableName:          "orders",
		Definition:         TypedTableAggregateDefinition{GroupBy: []string{"region"}},
		EstimatedStateRows: 1000,
	}
	catalog := []TypedTableAggregateArrangementInfo{{
		TableName:  "other",
		Definition: TypedTableAggregateDefinition{GroupBy: []string{"region"}},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		advice := AdviseTypedTableAggregateArrangement(catalog, request, TypedTableArrangementAdvisorOptions{})
		if advice.Action != TypedTableArrangementAdvisorCreate {
			b.Fatalf("Action = %q, want create", advice.Action)
		}
	}
}

func BenchmarkMU11JoinArrangementAdvisorCreate(b *testing.B) {
	request := TypedTableJoinArrangementRequest{
		LeftTableName:      "orders",
		RightTableName:     "customers",
		Definition:         TypedTableJoinDefinition{LeftField: "customer_id", RightField: "id"},
		EstimatedStateRows: 1000,
	}
	catalog := []TypedTableJoinArrangementInfo{{
		LeftTableName:  "other_left",
		RightTableName: "other_right",
		Definition:     request.Definition,
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		advice := AdviseTypedTableJoinArrangement(catalog, request, TypedTableArrangementAdvisorOptions{})
		if advice.Action != TypedTableArrangementAdvisorCreate {
			b.Fatalf("Action = %q, want create", advice.Action)
		}
	}
}
