package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTU26ExplainSQLIndexStrategySelectsDeterministically(t *testing.T) {
	decision, err := ExplainSQLIndexStrategy("people", "id", SQLIndexHint{}, []SQLIndexStrategyCandidate{
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people", Field: "id", Kind: "ORDERED"},
			Priority:           2,
			EstimatedRows:      4,
			EstimatedCost:      8,
			Available:          true,
		},
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people", Field: "id", Kind: "HASH"},
			Priority:           1,
			EstimatedRows:      8,
			EstimatedCost:      12,
			Available:          true,
		},
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people", Field: "name", Kind: "TEXT"},
			Priority:           0,
			EstimatedRows:      1,
			EstimatedCost:      1,
			Available:          true,
		},
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people", Field: "id", Kind: "BITMAP"},
			Priority:           0,
			EstimatedRows:      1,
			EstimatedCost:      2,
			Available:          false,
		},
	})
	if err != nil {
		t.Fatalf("ExplainSQLIndexStrategy: %v", err)
	}
	if !decision.HasSelection || decision.Selected.Kind != "HASH" {
		t.Fatalf("selected = %+v, want HASH", decision)
	}
	if decision.Reason != SQLIndexStrategyReasonPriority {
		t.Fatalf("reason = %q, want %q", decision.Reason, SQLIndexStrategyReasonPriority)
	}
	if len(decision.Candidates) != 4 {
		t.Fatalf("candidate count = %d, want 4", len(decision.Candidates))
	}
	if decision.Candidates[0].Kind != "HASH" || !decision.Candidates[0].Eligible {
		t.Fatalf("first candidate = %+v", decision.Candidates[0])
	}
	if decision.Candidates[1].Kind != "ORDERED" || decision.Candidates[1].Reason != SQLIndexStrategyReasonLowerPriority {
		t.Fatalf("second candidate = %+v", decision.Candidates[1])
	}
	if decision.Candidates[2].Kind != "BITMAP" || decision.Candidates[2].Reason != SQLIndexStrategyReasonUnavailable {
		t.Fatalf("third candidate = %+v", decision.Candidates[2])
	}
	if decision.Candidates[3].Kind != "TEXT" || decision.Candidates[3].Reason != SQLIndexStrategyReasonFieldMismatch {
		t.Fatalf("fourth candidate = %+v", decision.Candidates[3])
	}
}

func TestTU26ExplainSQLIndexStrategyHonorsSpecificHint(t *testing.T) {
	candidates := []SQLIndexStrategyCandidate{
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people", Field: "id", Kind: "HASH"},
			Available:          true,
		},
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people", Field: "id", Kind: "ORDERED"},
			Available:          true,
		},
	}
	decision, err := ExplainSQLIndexStrategy("people", "id", SQLIndexHint{
		Source: "people",
		Field:  "id",
		Kind:   "ORDERED",
		Mode:   SQLIndexHintForce,
	}, candidates)
	if err != nil {
		t.Fatalf("forced strategy: %v", err)
	}
	if !decision.HasSelection || decision.Selected.Kind != "ORDERED" {
		t.Fatalf("forced selection = %+v", decision)
	}
	if decision.Reason != SQLIndexStrategyReasonForced {
		t.Fatalf("forced reason = %q", decision.Reason)
	}
	if decision.Candidates[0].Reason != SQLIndexStrategyReasonKindMismatch {
		t.Fatalf("non-selected strategy = %+v", decision.Candidates[0])
	}

	decision, err = ExplainSQLIndexStrategy("people", "id", SQLIndexHint{
		Field: "id",
		Kind:  "HASH",
		Mode:  SQLIndexHintForbid,
	}, candidates)
	if err != nil {
		t.Fatalf("forbidden strategy: %v", err)
	}
	if !decision.HasSelection || decision.Selected.Kind != "ORDERED" {
		t.Fatalf("forbidden selection = %+v", decision)
	}
	if decision.Candidates[0].Reason != SQLIndexStrategyReasonForbidden {
		t.Fatalf("forbidden candidate = %+v", decision.Candidates[0])
	}
}

func TestTU26ExplainSQLIndexStrategyRejectsUnsupportedForcedHint(t *testing.T) {
	decision, err := ExplainSQLIndexStrategy("people", "id", SQLIndexHint{
		Field: "id",
		Kind:  "HASH",
		Mode:  SQLIndexHintForce,
	}, []SQLIndexStrategyCandidate{
		{
			SQLIndexDefinition: SQLIndexDefinition{Key: "people", Field: "id", Kind: "ORDERED"},
			Available:          true,
		},
	})
	if !errors.Is(err, ErrSQLIndexStrategyHintUnsupported) {
		t.Fatalf("error = %v, want %v", err, ErrSQLIndexStrategyHintUnsupported)
	}
	if decision.HasSelection || decision.Reason != SQLIndexStrategyReasonNoForcedCandidate {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestTU26ForcedStrategyUsesOptionalResolverAndExplainsIt(t *testing.T) {
	resolver := &tu26StrategyResolver{}
	result, err := ExecuteSQLQueryParameters(context.Background(),
		`FROM CACHE('people') AS p WHERE p.id = 1 SELECT p.name`, resolver, nil, SQLQueryOptions{
			IndexHint: SQLIndexHint{Source: "p", Field: "id", Kind: "HASH", Mode: SQLIndexHintForce},
		})
	if err != nil {
		t.Fatalf("forced strategy query: %v", err)
	}
	if !reflect.DeepEqual(resolver.strategies, []string{"HASH", "HASH"}) {
		t.Fatalf("strategy calls = %v", resolver.strategies)
	}
	if len(result.Rows) != 1 || result.Rows[0]["name"] != "alice" {
		t.Fatalf("rows = %#v", result.Rows)
	}
}

func TestTU26ForcedStrategyRejectsResolverWithoutStrategySupport(t *testing.T) {
	_, err := ExecuteSQLQueryParameters(context.Background(),
		`FROM CACHE('people') AS p WHERE p.id = 1 SELECT p.name`, tu26GenericIndexResolver{}, nil, SQLQueryOptions{
			IndexHint: SQLIndexHint{Field: "id", Kind: "HASH", Mode: SQLIndexHintForce},
		})
	if !errors.Is(err, ErrSQLIndexStrategyHintUnsupported) {
		t.Fatalf("error = %v, want %v", err, ErrSQLIndexStrategyHintUnsupported)
	}
}

func TestTU26ForbiddenStrategyDoesNotSilentlyFallBack(t *testing.T) {
	_, err := ExecuteSQLQueryParameters(context.Background(),
		`FROM CACHE('people') AS p WHERE p.id = 1 SELECT p.name`, tu26GenericIndexResolver{}, nil, SQLQueryOptions{
			IndexHint: SQLIndexHint{Field: "id", Kind: "HASH", Mode: SQLIndexHintForbid},
		})
	if !errors.Is(err, ErrSQLIndexStrategyHintUnsupported) {
		t.Fatalf("error = %v, want %v", err, ErrSQLIndexStrategyHintUnsupported)
	}
}

type tu26StrategyResolver struct {
	strategies []string
}

func (resolver *tu26StrategyResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1), "name": "alice"}}, nil
}

func (resolver *tu26StrategyResolver) ResolveSQLIndexedSource(string, string, string, interface{}) ([]Row, bool, error) {
	return []Row{{"id": int64(1), "name": "alice"}}, true, nil
}

func (resolver *tu26StrategyResolver) ResolveSQLIndexedSourceWithStrategy(_, _, _, strategy string, _ interface{}) ([]Row, bool, error) {
	resolver.strategies = append(resolver.strategies, strategy)
	return []Row{{"id": int64(1), "name": "alice"}}, true, nil
}

type tu26GenericIndexResolver struct{}

func (tu26GenericIndexResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1), "name": "alice"}}, nil
}

func (tu26GenericIndexResolver) ResolveSQLIndexedSource(string, string, string, interface{}) ([]Row, bool, error) {
	return []Row{{"id": int64(1), "name": "alice"}}, true, nil
}
