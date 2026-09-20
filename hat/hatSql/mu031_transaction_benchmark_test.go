package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkMU031TransactionalAggregate(b *testing.B) {
	const valuesPerBatch = 32

	b.Run("direct-batch", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			state := &mu031SerializableSum{}
			for value := int64(0); value < valuesPerBatch; value++ {
				if err := state.Add(value); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("transaction-batch", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			transaction, err := hatSql.NewSQLTransactionalAggregateState(&mu031SerializableSum{})
			if err != nil {
				b.Fatal(err)
			}
			if err := transaction.Run(func(state hatSql.SQLAggregateState) error {
				for value := int64(0); value < valuesPerBatch; value++ {
					if err := state.Add(value); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
