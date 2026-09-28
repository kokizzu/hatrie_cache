package hatSql

import "testing"

func BenchmarkM247ResumeValidation(b *testing.B) {
	checkpoint := m247BenchmarkCheckpoint()
	validator := QuerySubscriptionCheckpointValidatorFunc(func(QuerySubscriptionCheckpoint) error { return nil })

	b.Run("legacy", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			subscription, err := NewQuerySubscriptions(1).Resume(checkpoint)
			if err != nil {
				b.Fatal(err)
			}
			subscription.Close()
		}
	})
	b.Run("validated", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			subscription, err := NewQuerySubscriptions(1).ResumeWithValidator(checkpoint, validator)
			if err != nil {
				b.Fatal(err)
			}
			subscription.Close()
		}
	})
}

func m247BenchmarkCheckpoint() QuerySubscriptionCheckpoint {
	return QuerySubscriptionCheckpoint{
		Version: 1,
		Definition: QuerySubscriptionDefinition{
			Query:        "FROM CACHE('people') SELECT name",
			Dependencies: []string{"people"},
			AsOf:         10,
		},
		Snapshot: QuerySubscriptionSnapshot{Revision: 1, Frontier: 20},
	}
}
