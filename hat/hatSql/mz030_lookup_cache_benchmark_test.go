package hatSql

import (
	"context"
	"testing"
)

type mz030LookupBenchmarkResolver struct {
	orders      []Row
	countries   map[string]Row
	lookupCalls int
}

func (resolver *mz030LookupBenchmarkResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name == "CACHE" && key == "orders" {
		return resolver.orders, nil
	}
	return nil, nil
}

func (resolver *mz030LookupBenchmarkResolver) ResolveSQLLookupSource(name, key, field string, value interface{}) ([]Row, bool, error) {
	resolver.lookupCalls++
	if name != "EXTERNAL" || key != "countries" || field != "code" {
		return nil, false, nil
	}
	if value == nil {
		return nil, true, nil
	}
	code, ok := value.(string)
	if !ok {
		return nil, true, nil
	}
	row, ok := resolver.countries[code]
	if !ok {
		return nil, true, nil
	}
	return []Row{row}, true, nil
}

func (resolver *mz030LookupBenchmarkResolver) ResolveSQLExternalSource(string) ([]Row, error) {
	return nil, nil
}

func (resolver *mz030LookupBenchmarkResolver) SQLSourceFrontier(name, key string) (uint64, bool, bool, error) {
	if name != "EXTERNAL" || key != "countries" {
		return 0, false, false, nil
	}
	return 1, true, true, nil
}

func newMZ030LookupBenchmarkResolver() *mz030LookupBenchmarkResolver {
	orders := make([]Row, 512)
	for index := range orders {
		code := "SG"
		if index%2 != 0 {
			code = "JP"
		}
		orders[index] = Row{"id": int64(index), "country": code}
	}
	return &mz030LookupBenchmarkResolver{
		orders: orders,
		countries: map[string]Row{
			"SG": {"code": "SG", "name": "Singapore"},
			"JP": {"code": "JP", "name": "Japan"},
		},
	}
}

const mz030LookupJoinQuery = `
FROM CACHE('orders') AS order_row
LEFT JOIN EXTERNAL('countries') AS country ON order_row.country = country.code
SELECT order_row.id, country.name`

func BenchmarkMZ030LookupJoinBaseline(b *testing.B) {
	resolver := newMZ030LookupBenchmarkResolver()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteQueryParameters(context.Background(), mz030LookupJoinQuery, resolver, nil, QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(resolver.orders) {
			b.Fatalf("rows = %d, want %d", len(result.Rows), len(resolver.orders))
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(resolver.lookupCalls)/float64(b.N), "lookup_calls/op")
}

func BenchmarkMZ030LookupJoinFrontierCache(b *testing.B) {
	resolver := newMZ030LookupBenchmarkResolver()
	cache := NewSQLLookupJoinCache(8)
	result, err := ExecuteQueryParameters(context.Background(), mz030LookupJoinQuery, resolver, nil, QueryOptions{LookupJoinCache: cache})
	if err != nil {
		b.Fatal(err)
	}
	if len(result.Rows) != len(resolver.orders) {
		b.Fatalf("warmup rows = %d, want %d", len(result.Rows), len(resolver.orders))
	}
	resolver.lookupCalls = 0
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteQueryParameters(context.Background(), mz030LookupJoinQuery, resolver, nil, QueryOptions{LookupJoinCache: cache})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(resolver.orders) {
			b.Fatalf("rows = %d, want %d", len(result.Rows), len(resolver.orders))
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(resolver.lookupCalls)/float64(b.N), "lookup_calls/op")
}
