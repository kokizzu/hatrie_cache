package hatDataStructure

import (
	"strconv"
	"testing"
)

type tu26ManualStrategy struct {
	name       string
	field      string
	equality   bool
	rangeQuery bool
	bytes      uint64
}

var tu26BenchmarkSinkName string

func tu26ManualStrategies() []tu26ManualStrategy {
	strategies := make([]tu26ManualStrategy, 64)
	for index := range strategies {
		strategies[index] = tu26ManualStrategy{
			name:       "index-" + strconv.Itoa(index),
			field:      "field-" + strconv.Itoa(index),
			equality:   index%2 == 0,
			rangeQuery: index%2 == 1,
			bytes:      uint64((index + 1) << 20),
		}
	}
	strategies[len(strategies)-1] = tu26ManualStrategy{name: "orders_by_created_at", field: "created_at", rangeQuery: true, bytes: 24 << 20}
	return strategies
}

func BenchmarkTU26ManualStrategyScan(b *testing.B) {
	strategies := tu26ManualStrategies()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for _, strategy := range strategies {
			if strategy.name == "orders_by_created_at" && strategy.rangeQuery {
				tu26BenchmarkSinkName = strategy.name
				break
			}
		}
	}
}

func BenchmarkTU26CatalogResolve(b *testing.B) {
	catalog := tu26BenchmarkCatalog(b)
	hint := IndexStrategyHint{Name: "orders_by_created_at", Field: "created_at", Operation: IndexOperationRange}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		resolved, err := catalog.Resolve(hint)
		if err != nil {
			b.Fatal(err)
		}
		tu26BenchmarkSinkName = resolved.Name
	}
}

func BenchmarkTU26CatalogResolveInto(b *testing.B) {
	catalog := tu26BenchmarkCatalog(b)
	hint := IndexStrategyHint{Name: "orders_by_created_at", Field: "created_at", Operation: IndexOperationRange}
	destination := IndexStrategyDescriptor{Fields: make([]string, 0, 1)}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := catalog.ResolveInto(&destination, hint); err != nil {
			b.Fatal(err)
		}
		tu26BenchmarkSinkName = destination.Name
	}
}

func BenchmarkTU26CatalogSuggest(b *testing.B) {
	catalog := tu26BenchmarkCatalog(b)
	hint := IndexStrategyHint{Field: "created_at", Operation: IndexOperationRange}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		candidates, err := catalog.Suggest(hint)
		if err != nil {
			b.Fatal(err)
		}
		if len(candidates) != 0 {
			tu26BenchmarkSinkName = candidates[0].Name
		}
	}
}

func BenchmarkTU26CatalogSuggestInto(b *testing.B) {
	catalog := tu26BenchmarkCatalog(b)
	hint := IndexStrategyHint{Field: "created_at", Operation: IndexOperationRange}
	destination := make([]IndexStrategyDescriptor, 0, len(tu26ManualStrategies()))
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		var err error
		destination, err = catalog.SuggestInto(destination, hint)
		if err != nil {
			b.Fatal(err)
		}
		if len(destination) != 1 {
			b.Fatalf("candidate count = %d, want 1", len(destination))
		}
		tu26BenchmarkSinkName = destination[0].Name
	}
}

func tu26BenchmarkCatalog(b testing.TB) *IndexStrategyCatalog {
	b.Helper()
	catalog := NewDefaultIndexStrategyCatalog()
	for _, strategy := range tu26ManualStrategies() {
		capabilities := IndexStrategyCapabilities{Equality: strategy.equality, Range: strategy.rangeQuery, Ordered: strategy.rangeQuery}
		if err := catalog.Register(IndexStrategyDescriptor{
			Name:                 strategy.name,
			Kind:                 IndexStrategyOrdered,
			Fields:               []string{strategy.field},
			Capabilities:         capabilities,
			EstimatedCardinality: 1_000_000,
			EstimatedBytes:       strategy.bytes,
		}); err != nil {
			b.Fatal(err)
		}
	}
	return catalog
}
