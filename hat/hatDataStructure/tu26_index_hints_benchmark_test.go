package hatDataStructure

import "testing"

var tu26IndexSelectionSink IndexSelectionDecision
var tu26IndexSelectionReportSink IndexSelectionReport

func tu26IndexSelectionCatalog(b *testing.B) *IndexSelectionCatalog {
	b.Helper()
	catalog, err := NewIndexSelectionCatalog([]IndexCandidate{
		{Name: "account", Strategy: IndexStrategyHash, EstimatedCost: 20, Capabilities: IndexCapabilityEquality},
		{Name: "created", Strategy: IndexStrategyOrdered, EstimatedCost: 30, Capabilities: IndexCapabilityRange},
		{Name: "email", Strategy: IndexStrategyOrdered, EstimatedCost: 40, Capabilities: IndexCapabilityEquality | IndexCapabilityPrefix},
		{Name: "text-search", Strategy: IndexStrategyFullText, EstimatedCost: 50, Capabilities: IndexCapabilityContains},
	})
	if err != nil {
		b.Fatal(err)
	}
	return catalog
}

func BenchmarkTU26IndexHintSelection(b *testing.B) {
	catalog := tu26IndexSelectionCatalog(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decision, err := catalog.Select(IndexOperationEquality, IndexHint{})
		if err != nil {
			b.Fatal(err)
		}
		tu26IndexSelectionSink = decision
	}
}

func BenchmarkTU26IndexHintExplanation(b *testing.B) {
	catalog := tu26IndexSelectionCatalog(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		report, err := catalog.Explain(IndexOperationEquality, IndexHint{})
		if err != nil {
			b.Fatal(err)
		}
		tu26IndexSelectionReportSink = report
	}
}
