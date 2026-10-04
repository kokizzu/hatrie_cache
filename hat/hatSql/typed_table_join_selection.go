package hatSql

import "fmt"

const maxTypedTableJoinArrangementCandidates = 256

// TypedTableJoinArrangementCandidate describes one compatible equi-join that
// a changing query predicate may use. EstimatedStateRows is an expected
// retained pair count used only for deterministic selection; it does not
// change join semantics.
type TypedTableJoinArrangementCandidate struct {
	Definition         TypedTableJoinDefinition
	EstimatedStateRows uint64
}

// TypedTableJoinArrangementSelection reports the candidate acquired by
// AcquireBest. Reused is true when the registry already maintained that
// definition, so callers can avoid treating a lease as a new arrangement.
type TypedTableJoinArrangementSelection struct {
	Definition         TypedTableJoinDefinition
	EstimatedStateRows uint64
	Reused             bool
}

// AcquireBest chooses and acquires the smallest compatible join arrangement.
// Ties reuse an already-maintained arrangement, then use field names for a
// deterministic result. Only the selected arrangement is created or retained.
// The method is opt-in; Acquire remains the zero-overhead exact-definition
// path for callers that already know their join key.
func (arrangements *TypedTableJoinArrangements) AcquireBest(candidates []TypedTableJoinArrangementCandidate) (*TypedTableJoinArrangement, TypedTableJoinArrangementSelection, error) {
	if arrangements == nil {
		return nil, TypedTableJoinArrangementSelection{}, fmt.Errorf("typed table join arrangements are nil")
	}
	if len(candidates) == 0 {
		return nil, TypedTableJoinArrangementSelection{}, fmt.Errorf("typed table join arrangement candidates are required")
	}
	if len(candidates) > maxTypedTableJoinArrangementCandidates {
		return nil, TypedTableJoinArrangementSelection{}, fmt.Errorf("typed table join arrangement candidates exceed %d", maxTypedTableJoinArrangementCandidates)
	}

	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	var selected TypedTableJoinArrangementCandidate
	selectedKey := ""
	selectedExisting := false
	selectedSet := false
	for _, candidate := range candidates {
		if !typedTableJoinDefinitionCompatible(arrangements.left, arrangements.right, candidate.Definition) {
			continue
		}
		if !selectedSet || candidate.EstimatedStateRows < selected.EstimatedStateRows {
			selected = candidate
			selectedKey = ""
			selectedExisting = false
			selectedSet = true
			continue
		}
		if candidate.EstimatedStateRows > selected.EstimatedStateRows {
			continue
		}
		key := typedTableArrangementAdvisorJoinDefinitionKey(candidate.Definition)
		existing := arrangements.entries[key] != nil
		if selectedKey == "" {
			selectedKey = typedTableArrangementAdvisorJoinDefinitionKey(selected.Definition)
			selectedExisting = arrangements.entries[selectedKey] != nil
		}
		if typedTableJoinArrangementCandidateBetter(candidate, existing, key, selected, selectedExisting, selectedKey) {
			selected = candidate
			selectedKey = key
			selectedExisting = existing
		}
	}
	if !selectedSet {
		return nil, TypedTableJoinArrangementSelection{}, fmt.Errorf("typed table join arrangement has no compatible candidates")
	}
	if selectedKey == "" {
		selectedKey = typedTableArrangementAdvisorJoinDefinitionKey(selected.Definition)
		selectedExisting = arrangements.entries[selectedKey] != nil
	}
	arrangement, err := arrangements.acquireLockedKey(selected.Definition, selectedKey)
	if err != nil {
		return nil, TypedTableJoinArrangementSelection{}, err
	}
	return arrangement, TypedTableJoinArrangementSelection{
		Definition:         selected.Definition,
		EstimatedStateRows: selected.EstimatedStateRows,
		Reused:             selectedExisting,
	}, nil
}

func typedTableJoinArrangementCandidateBetter(candidate TypedTableJoinArrangementCandidate, candidateExisting bool, candidateKey string, selected TypedTableJoinArrangementCandidate, selectedExisting bool, selectedKey string) bool {
	if candidate.EstimatedStateRows != selected.EstimatedStateRows {
		return candidate.EstimatedStateRows < selected.EstimatedStateRows
	}
	if candidateExisting != selectedExisting {
		return candidateExisting
	}
	return candidateKey < selectedKey
}

func typedTableJoinDefinitionCompatible(left, right *TypedTable, definition TypedTableJoinDefinition) bool {
	if left == nil || right == nil || definition.LeftField == "" || definition.RightField == "" {
		return false
	}
	_, leftKind, leftFound := typedTableJoinField(left, definition.LeftField)
	_, rightKind, rightFound := typedTableJoinField(right, definition.RightField)
	return leftFound && rightFound && leftKind == rightKind
}
