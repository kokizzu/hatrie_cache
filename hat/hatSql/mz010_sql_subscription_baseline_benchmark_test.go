//go:build mz010baseline

package hatSql

import "testing"

func BenchmarkMZ010ManualSubscription(b *testing.B) {
	rows := map[string][]Row{"people": {{"id": 1, "name": "Ada"}}}
	resolver := SourceResolverFunc(func(_ string, key string) ([]Row, error) {
		return rows[key], nil
	})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		registry := NewQuerySubscriptions(1)
		subscription, err := registry.Subscribe(nil, QuerySubscriptionDefinition{
			Query:        `FROM CACHE('people') SELECT id, name`,
			Dependencies: []string{"people"},
		}, resolver, QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		subscription.Close()
	}
}
