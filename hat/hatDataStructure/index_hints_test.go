package hatDataStructure

import (
	"errors"
	"testing"
)

func TestIndexHintSelectsLowestCostAndExplains(t *testing.T) {
	catalog, err := NewIndexSelectionCatalog([]IndexCandidate{
		{Name: "ordered-created", Strategy: IndexStrategyOrdered, EstimatedCost: 90, Capabilities: IndexCapabilityEquality | IndexCapabilityRange},
		{Name: "hash-account", Strategy: IndexStrategyHash, EstimatedCost: 12, Capabilities: IndexCapabilityEquality},
		{Name: "text-search", Strategy: IndexStrategyFullText, EstimatedCost: 30, Capabilities: IndexCapabilityContains},
		{Name: "fallback", Strategy: IndexStrategyHash, EstimatedCost: 300, Capabilities: IndexCapabilityEquality},
	})
	if err != nil {
		t.Fatal(err)
	}

	decision, err := catalog.Select(IndexOperationEquality, IndexHint{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Candidate.Name != "hash-account" || decision.Reason != IndexSelectionBestCost || decision.Eligible != 3 || decision.Considered != 4 {
		t.Fatalf("decision = %#v, want hash-account/best-cost/3/4", decision)
	}

	report, err := catalog.Explain(IndexOperationEquality, IndexHint{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Decision.Candidate.Name != "hash-account" || len(report.Evaluations) != 4 {
		t.Fatalf("report = %#v, want four evaluations and hash-account", report)
	}
	if report.Evaluations[2].Rejection != IndexSelectionUnsupportedOperation {
		t.Fatalf("text rejection = %v, want unsupported operation", report.Evaluations[2].Rejection)
	}
	if report.Evaluations[3].Rejection != IndexSelectionHigherCost {
		t.Fatalf("fallback rejection = %v, want higher cost", report.Evaluations[3].Rejection)
	}
}

func TestIndexHintRequiredNameAndStrategy(t *testing.T) {
	catalog, err := NewIndexSelectionCatalog([]IndexCandidate{
		{Name: "created", Strategy: IndexStrategyOrdered, EstimatedCost: 10, Capabilities: IndexCapabilityRange},
		{Name: "account", Strategy: IndexStrategyHash, EstimatedCost: 20, Capabilities: IndexCapabilityEquality},
	})
	if err != nil {
		t.Fatal(err)
	}

	decision, err := catalog.Select(IndexOperationRange, IndexHint{Name: "created", Required: true})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Candidate.Name != "created" || decision.Reason != IndexSelectionHintName {
		t.Fatalf("name hint decision = %#v", decision)
	}
	if _, err := catalog.Select(IndexOperationEquality, IndexHint{Strategy: IndexStrategyOrdered, Required: true}); !errors.Is(err, ErrIndexHintUnsupported) {
		t.Fatalf("unsupported required strategy error = %v, want unsupported", err)
	}
	decision, err = catalog.Select(IndexOperationEquality, IndexHint{Strategy: IndexStrategyOrdered})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Candidate.Name != "account" || decision.Reason != IndexSelectionHintFallback || !decision.Fallback {
		t.Fatalf("optional strategy fallback = %#v, want account/fallback", decision)
	}
}

func TestIndexHintRequiredMissingAndValidation(t *testing.T) {
	if _, err := NewIndexSelectionCatalog(nil); !errors.Is(err, ErrIndexHintNoCandidate) {
		t.Fatalf("empty catalog error = %v, want no candidate", err)
	}
	if _, err := NewIndexSelectionCatalog([]IndexCandidate{{Name: "same", Strategy: IndexStrategyHash, Capabilities: IndexCapabilityEquality}, {Name: "same", Strategy: IndexStrategyOrdered, Capabilities: IndexCapabilityEquality}}); !errors.Is(err, ErrIndexHintDuplicate) {
		t.Fatalf("duplicate error = %v, want duplicate", err)
	}
	if _, err := NewIndexSelectionCatalog([]IndexCandidate{{Name: "bad", Strategy: IndexStrategyInvalid, Capabilities: IndexCapabilityEquality}}); !errors.Is(err, ErrIndexHintInvalid) {
		t.Fatalf("invalid strategy error = %v, want invalid", err)
	}

	catalog, err := NewIndexSelectionCatalog([]IndexCandidate{{Name: "account", Strategy: IndexStrategyHash, Capabilities: IndexCapabilityEquality}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Select(IndexOperationRange, IndexHint{Name: "missing", Required: true}); !errors.Is(err, ErrIndexHintNotFound) {
		t.Fatalf("missing name error = %v, want not found", err)
	}
	if _, err := catalog.Select(IndexOperation(0), IndexHint{}); !errors.Is(err, ErrIndexHintInvalid) {
		t.Fatalf("invalid operation error = %v, want invalid", err)
	}
}

func TestIndexHintTieBreaksByName(t *testing.T) {
	catalog, err := NewIndexSelectionCatalog([]IndexCandidate{
		{Name: "zeta", Strategy: IndexStrategyHash, EstimatedCost: 10, Capabilities: IndexCapabilityEquality},
		{Name: "alpha", Strategy: IndexStrategyOrdered, EstimatedCost: 10, Capabilities: IndexCapabilityEquality},
	})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := catalog.Select(IndexOperationEquality, IndexHint{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Candidate.Name != "alpha" || decision.Reason != IndexSelectionTieBreak {
		t.Fatalf("tie decision = %#v, want alpha/tie-break", decision)
	}
}
