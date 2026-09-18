//go:build !mz010baseline

package hatSql

import (
	"context"
	"testing"
)

func benchmarkMZ010Subscription(b *testing.B, automatic bool) {
	rows := map[string][]Row{"people": {{"id": 1, "name": "Ada"}}}
	resolver := SourceResolverFunc(func(_ string, key string) ([]Row, error) {
		return rows[key], nil
	})
	source := `FROM CACHE('people') SELECT id, name`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		registry := NewQuerySubscriptions(1)
		var subscription *QuerySubscription
		var err error
		if automatic {
			subscription, err = registry.SubscribeSQL(context.Background(), QuerySubscriptionDefinition{Query: source}, resolver, QueryOptions{})
		} else {
			subscription, err = registry.Subscribe(context.Background(), QuerySubscriptionDefinition{
				Query:        source,
				Dependencies: []string{"people"},
			}, resolver, QueryOptions{})
		}
		if err != nil {
			b.Fatal(err)
		}
		subscription.Close()
	}
}

func BenchmarkMZ010ManualSubscription(b *testing.B) {
	benchmarkMZ010Subscription(b, false)
}

func BenchmarkMZ010AutoSubscription(b *testing.B) {
	benchmarkMZ010Subscription(b, true)
}
