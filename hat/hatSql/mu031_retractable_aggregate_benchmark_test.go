package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkMU031AggregateStateCreation(b *testing.B) {
	registry := hatSql.NewSQLAggregateCombinatorRegistry()
	retractable, err := hatSql.NewSQLAggregateCombinator("retract_sum", func() hatSql.SQLAggregateState {
		return &mu031RetractableSum{}
	})
	if err != nil {
		b.Fatal(err)
	}
	serializable, err := hatSql.NewSQLAggregateCombinator("serial_sum", func() hatSql.SQLAggregateState {
		return &mu031SerializableSum{}
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(retractable); err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(serializable); err != nil {
		b.Fatal(err)
	}

	for _, benchmark := range []struct {
		name string
		fn   func() (hatSql.SQLAggregateState, error)
	}{
		{
			name: "base_retractable",
			fn: func() (hatSql.SQLAggregateState, error) {
				return registry.NewState("retract_sum")
			},
		},
		{
			name: "capability_retractable",
			fn: func() (hatSql.SQLAggregateState, error) {
				return registry.NewRetractableState("retract_sum")
			},
		},
		{
			name: "base_serializable",
			fn: func() (hatSql.SQLAggregateState, error) {
				return registry.NewState("serial_sum")
			},
		},
		{
			name: "capability_serializable",
			fn: func() (hatSql.SQLAggregateState, error) {
				return registry.NewSerializableState("serial_sum")
			},
		},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := benchmark.fn(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
