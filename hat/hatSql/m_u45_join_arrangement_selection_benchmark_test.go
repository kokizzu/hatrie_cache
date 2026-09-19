//go:build mu45

package hatSql

import "testing"

func BenchmarkTypedTableJoinArrangementAdvisorExact(b *testing.B) {
	request, catalog, options := benchmarkTypedTableJoinArrangementSelectionFixture()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = AdviseTypedTableJoinArrangement(catalog, request.Alternatives[0], options)
	}
}

func BenchmarkSelectTypedTableJoinArrangementOneAlternative(b *testing.B) {
	request, catalog, options := benchmarkTypedTableJoinArrangementSelectionFixture()
	request.Alternatives = request.Alternatives[:1]
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := SelectTypedTableJoinArrangement(catalog, request, options); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectTypedTableJoinArrangementThreeAlternatives(b *testing.B) {
	request, catalog, options := benchmarkTypedTableJoinArrangementSelectionFixture()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := SelectTypedTableJoinArrangement(catalog, request, options); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkTypedTableJoinArrangementSelectionFixture() (
	TypedTableJoinArrangementSelectionRequest,
	[]TypedTableJoinArrangementInfo,
	TypedTableArrangementAdvisorOptions,
) {
	request := TypedTableJoinArrangementSelectionRequest{Alternatives: []TypedTableJoinArrangementRequest{
		{
			LeftTableName:      "orders",
			RightTableName:     "customers",
			Definition:         TypedTableJoinDefinition{LeftField: "customer_id", RightField: "id"},
			EstimatedStateRows: 200,
		},
		{
			LeftTableName:      "orders",
			RightTableName:     "customers",
			Definition:         TypedTableJoinDefinition{LeftField: "account_id", RightField: "id"},
			EstimatedStateRows: 200,
		},
		{
			LeftTableName:      "orders",
			RightTableName:     "customers",
			Definition:         TypedTableJoinDefinition{LeftField: "region_id", RightField: "id"},
			EstimatedStateRows: 200,
		},
	}}
	catalog := []TypedTableJoinArrangementInfo{
		{
			LeftTableName:       "orders",
			RightTableName:      "customers",
			Definition:          request.Alternatives[0].Definition,
			LeftCheckpoint:      97,
			LeftSourceSequence:  100,
			RightCheckpoint:     98,
			RightSourceSequence: 100,
		},
		{
			LeftTableName:       "orders",
			RightTableName:      "customers",
			Definition:          request.Alternatives[1].Definition,
			LeftCheckpoint:      100,
			LeftSourceSequence:  100,
			RightCheckpoint:     100,
			RightSourceSequence: 100,
		},
	}
	options := TypedTableArrangementAdvisorOptions{
		JoinStateBytesPerRow:  128,
		FixedArrangementBytes: 256,
		ReplayBytesPerChange:  64,
	}
	return request, catalog, options
}
