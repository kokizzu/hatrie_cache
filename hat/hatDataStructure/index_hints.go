package hatDataStructure

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// IndexStrategy identifies the access method an index uses.
type IndexStrategy uint8

const (
	IndexStrategyInvalid IndexStrategy = iota
	IndexStrategyHash
	IndexStrategyOrdered
	IndexStrategyFunctional
	IndexStrategyMultikey
	IndexStrategyBitset
	IndexStrategyFullText
)

// IndexCapabilityMask describes the operations an index can accelerate.
type IndexCapabilityMask uint8

const (
	IndexCapabilityEquality IndexCapabilityMask = 1 << iota
	IndexCapabilityRange
	IndexCapabilityPrefix
	IndexCapabilityContains
)

// IndexOperation is the operation for which an index is selected.
type IndexOperation uint8

const (
	IndexOperationInvalid IndexOperation = iota
	IndexOperationEquality
	IndexOperationRange
	IndexOperationPrefix
	IndexOperationContains
)

// IndexCandidate is the planner metadata for one available index.
// EstimatedCost is an ordinal cost supplied by the caller. Lower values win.
type IndexCandidate struct {
	Name          string
	Strategy      IndexStrategy
	EstimatedCost uint64
	Capabilities  IndexCapabilityMask
}

// IndexHint optionally constrains index selection. A required hint returns an
// error when it cannot be honored; an optional hint falls back to normal
// selection.
type IndexHint struct {
	Name     string
	Strategy IndexStrategy
	Required bool
}

// IndexSelectionReason explains why the selected candidate won.
type IndexSelectionReason uint8

const (
	IndexSelectionReasonInvalid IndexSelectionReason = iota
	IndexSelectionBestCost
	IndexSelectionHintName
	IndexSelectionHintStrategy
	IndexSelectionHintFallback
	IndexSelectionTieBreak
)

// IndexSelectionRejection explains why a candidate was not eligible.
type IndexSelectionRejection uint8

const (
	IndexSelectionRejectionNone IndexSelectionRejection = iota
	IndexSelectionUnsupportedOperation
	IndexSelectionHigherCost
	IndexSelectionHintMismatch
)

// IndexSelectionDecision is the compact result returned by Select.
type IndexSelectionDecision struct {
	Candidate  IndexCandidate
	Operation  IndexOperation
	Reason     IndexSelectionReason
	Considered int
	Eligible   int
	Fallback   bool
}

// IndexCandidateEvaluation is the per-candidate detail returned by Explain.
type IndexCandidateEvaluation struct {
	Candidate IndexCandidate
	Eligible  bool
	Rejection IndexSelectionRejection
}

// IndexSelectionReport combines the selected candidate with explain details.
type IndexSelectionReport struct {
	Decision    IndexSelectionDecision
	Evaluations []IndexCandidateEvaluation
}

var (
	ErrIndexHintNoCandidate = errors.New("index hint: no candidate")
	ErrIndexHintDuplicate   = errors.New("index hint: duplicate candidate")
	ErrIndexHintInvalid     = errors.New("index hint: invalid metadata")
	ErrIndexHintUnsupported = errors.New("index hint: unsupported operation")
	ErrIndexHintNotFound    = errors.New("index hint: candidate not found")
)

// IndexSelectionCatalog is an immutable, validated set of index candidates.
// Construct one when the schema or index set changes and reuse it for reads.
type IndexSelectionCatalog struct {
	candidates []IndexCandidate
}

// NewIndexSelectionCatalog validates and copies candidates so later caller
// mutations cannot change planner behavior.
func NewIndexSelectionCatalog(candidates []IndexCandidate) (*IndexSelectionCatalog, error) {
	if len(candidates) == 0 {
		return nil, ErrIndexHintNoCandidate
	}

	validated := make([]IndexCandidate, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for i, candidate := range candidates {
		name, err := normalizeIndexName(candidate.Name)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[name]; exists {
			return nil, ErrIndexHintDuplicate
		}
		if !isValidIndexStrategy(candidate.Strategy) || !isValidCapabilities(candidate.Capabilities) {
			return nil, ErrIndexHintInvalid
		}
		seen[name] = struct{}{}
		candidate.Name = name
		validated[i] = candidate
	}

	return &IndexSelectionCatalog{candidates: validated}, nil
}

// Select returns the lowest-cost candidate compatible with operation. It does
// not allocate after catalog construction for normalized hints.
func (catalog *IndexSelectionCatalog) Select(operation IndexOperation, hint IndexHint) (IndexSelectionDecision, error) {
	return catalog.selectIndex(operation, hint)
}

// Explain returns the same decision as Select plus a bounded explanation for
// every candidate. It is intended for diagnostics and query-plan inspection.
func (catalog *IndexSelectionCatalog) Explain(operation IndexOperation, hint IndexHint) (IndexSelectionReport, error) {
	decision, err := catalog.selectIndex(operation, hint)
	if err != nil {
		return IndexSelectionReport{}, err
	}

	report := IndexSelectionReport{
		Decision:    decision,
		Evaluations: make([]IndexCandidateEvaluation, len(catalog.candidates)),
	}
	normalizedHint, _ := normalizeIndexHint(hint)
	for i, candidate := range catalog.candidates {
		evaluation := IndexCandidateEvaluation{Candidate: candidate}
		if !supportsOperation(candidate.Capabilities, operation) {
			evaluation.Rejection = IndexSelectionUnsupportedOperation
			report.Evaluations[i] = evaluation
			continue
		}
		evaluation.Eligible = true
		if !matchesIndexHint(candidate, normalizedHint) || (decision.Fallback && hasIndexHint(normalizedHint)) {
			evaluation.Rejection = IndexSelectionHintMismatch
		} else if candidate.Name != decision.Candidate.Name {
			evaluation.Rejection = IndexSelectionHigherCost
		}
		report.Evaluations[i] = evaluation
	}
	return report, nil
}

// SelectIndex is a convenience wrapper for callers that do not need to keep
// the catalog object themselves.
func SelectIndex(candidates []IndexCandidate, operation IndexOperation, hint IndexHint) (IndexSelectionDecision, error) {
	catalog, err := NewIndexSelectionCatalog(candidates)
	if err != nil {
		return IndexSelectionDecision{}, err
	}
	return catalog.Select(operation, hint)
}

// ExplainIndexSelection is the convenience counterpart to SelectIndex.
func ExplainIndexSelection(candidates []IndexCandidate, operation IndexOperation, hint IndexHint) (IndexSelectionReport, error) {
	catalog, err := NewIndexSelectionCatalog(candidates)
	if err != nil {
		return IndexSelectionReport{}, err
	}
	return catalog.Explain(operation, hint)
}

func (catalog *IndexSelectionCatalog) selectIndex(operation IndexOperation, hint IndexHint) (IndexSelectionDecision, error) {
	if catalog == nil || len(catalog.candidates) == 0 {
		return IndexSelectionDecision{}, ErrIndexHintNoCandidate
	}
	if !isValidIndexOperation(operation) {
		return IndexSelectionDecision{}, ErrIndexHintInvalid
	}
	normalizedHint, err := normalizeIndexHint(hint)
	if err != nil {
		return IndexSelectionDecision{}, err
	}
	if normalizedHint.Required && normalizedHint.Name != "" {
		foundName := false
		for _, candidate := range catalog.candidates {
			if candidate.Name == normalizedHint.Name {
				foundName = true
				break
			}
		}
		if !foundName {
			return IndexSelectionDecision{}, ErrIndexHintNotFound
		}
	}

	decision := IndexSelectionDecision{
		Operation:  operation,
		Considered: len(catalog.candidates),
	}
	for _, candidate := range catalog.candidates {
		if supportsOperation(candidate.Capabilities, operation) {
			decision.Eligible++
		}
	}
	if decision.Eligible == 0 {
		if normalizedHint.Required {
			return IndexSelectionDecision{}, ErrIndexHintUnsupported
		}
		return IndexSelectionDecision{}, ErrIndexHintNoCandidate
	}

	selected, found := selectMatchingCandidate(catalog.candidates, operation, normalizedHint)
	if !found && hasIndexHint(normalizedHint) {
		if normalizedHint.Required {
			if normalizedHint.Name != "" {
				for _, candidate := range catalog.candidates {
					if candidate.Name == normalizedHint.Name {
						return IndexSelectionDecision{}, ErrIndexHintUnsupported
					}
				}
				return IndexSelectionDecision{}, ErrIndexHintNotFound
			}
			return IndexSelectionDecision{}, ErrIndexHintUnsupported
		}
		selected, found = selectMatchingCandidate(catalog.candidates, operation, IndexHint{})
		decision.Fallback = true
		decision.Reason = IndexSelectionHintFallback
	}
	if !found {
		return IndexSelectionDecision{}, ErrIndexHintNoCandidate
	}

	decision.Candidate = selected
	if !decision.Fallback {
		switch {
		case normalizedHint.Name != "":
			decision.Reason = IndexSelectionHintName
		case normalizedHint.Strategy != IndexStrategyInvalid:
			decision.Reason = IndexSelectionHintStrategy
		default:
			decision.Reason = bestCostReason(catalog.candidates, operation, normalizedHint)
		}
	}
	return decision, nil
}

func selectMatchingCandidate(candidates []IndexCandidate, operation IndexOperation, hint IndexHint) (IndexCandidate, bool) {
	var selected IndexCandidate
	found := false
	for _, candidate := range candidates {
		if !supportsOperation(candidate.Capabilities, operation) || !matchesIndexHint(candidate, hint) {
			continue
		}
		if !found || candidate.EstimatedCost < selected.EstimatedCost ||
			(candidate.EstimatedCost == selected.EstimatedCost && lessIndexCandidate(candidate, selected)) {
			selected = candidate
			found = true
		}
	}
	return selected, found
}

func bestCostReason(candidates []IndexCandidate, operation IndexOperation, hint IndexHint) IndexSelectionReason {
	var selected IndexCandidate
	found := false
	tied := false
	for _, candidate := range candidates {
		if !supportsOperation(candidate.Capabilities, operation) || !matchesIndexHint(candidate, hint) {
			continue
		}
		if !found || candidate.EstimatedCost < selected.EstimatedCost {
			selected = candidate
			found = true
			tied = false
			continue
		}
		if candidate.EstimatedCost == selected.EstimatedCost {
			tied = true
			if lessIndexCandidate(candidate, selected) {
				selected = candidate
			}
		}
	}
	if tied {
		return IndexSelectionTieBreak
	}
	return IndexSelectionBestCost
}

func lessIndexCandidate(left, right IndexCandidate) bool {
	if left.Name != right.Name {
		return left.Name < right.Name
	}
	return left.Strategy < right.Strategy
}

func normalizeIndexName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || !utf8.ValidString(name) || len(name) > 256 {
		return "", ErrIndexHintInvalid
	}
	return name, nil
}

func normalizeIndexHint(hint IndexHint) (IndexHint, error) {
	if hint.Strategy != IndexStrategyInvalid && !isValidIndexStrategy(hint.Strategy) {
		return IndexHint{}, ErrIndexHintInvalid
	}
	if hint.Name != "" {
		name, err := normalizeIndexName(hint.Name)
		if err != nil {
			return IndexHint{}, err
		}
		hint.Name = name
	}
	if hint.Required && !hasIndexHint(hint) {
		return IndexHint{}, ErrIndexHintInvalid
	}
	return hint, nil
}

func hasIndexHint(hint IndexHint) bool {
	return hint.Name != "" || hint.Strategy != IndexStrategyInvalid
}

func matchesIndexHint(candidate IndexCandidate, hint IndexHint) bool {
	if hint.Name != "" && candidate.Name != hint.Name {
		return false
	}
	return hint.Strategy == IndexStrategyInvalid || candidate.Strategy == hint.Strategy
}

func supportsOperation(capabilities IndexCapabilityMask, operation IndexOperation) bool {
	return capabilities&capabilityForOperation(operation) != 0
}

func capabilityForOperation(operation IndexOperation) IndexCapabilityMask {
	switch operation {
	case IndexOperationEquality:
		return IndexCapabilityEquality
	case IndexOperationRange:
		return IndexCapabilityRange
	case IndexOperationPrefix:
		return IndexCapabilityPrefix
	case IndexOperationContains:
		return IndexCapabilityContains
	default:
		return 0
	}
}

func isValidIndexOperation(operation IndexOperation) bool {
	return operation >= IndexOperationEquality && operation <= IndexOperationContains
}

func isValidIndexStrategy(strategy IndexStrategy) bool {
	return strategy >= IndexStrategyHash && strategy <= IndexStrategyFullText
}

func isValidCapabilities(capabilities IndexCapabilityMask) bool {
	return capabilities != 0 && capabilities&^(IndexCapabilityEquality|IndexCapabilityRange|IndexCapabilityPrefix|IndexCapabilityContains) == 0
}
