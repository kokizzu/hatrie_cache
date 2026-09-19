package hatSql

import (
	"errors"
	"sort"
)

var (
	// ErrTypedTableJoinArrangementSelectionNoCandidates means that no join
	// predicate was supplied to the selector.
	ErrTypedTableJoinArrangementSelectionNoCandidates = errors.New("typed table join arrangement selection requires at least one candidate")
	// ErrTypedTableJoinArrangementSelectionTableMismatch means that candidate
	// predicates do not use the same left/right table orientation.
	ErrTypedTableJoinArrangementSelectionTableMismatch = errors.New("typed table join arrangement candidates must use the same table orientation")
	// ErrTypedTableJoinArrangementSelectionDuplicate means that the same exact
	// join predicate was supplied more than once.
	ErrTypedTableJoinArrangementSelectionDuplicate = errors.New("typed table join arrangement candidates contain a duplicate predicate")
)

// TypedTableJoinArrangementSelectionRequest describes explicit join
// predicates that are safe alternatives for one left/right table pair.
// Selection never infers equivalence between different predicates.
type TypedTableJoinArrangementSelectionRequest struct {
	Alternatives []TypedTableJoinArrangementRequest `json:"alternatives"`
}

// TypedTableJoinArrangementSelectionCandidate contains one alternative and
// the exact advisor result for that alternative.
type TypedTableJoinArrangementSelectionCandidate struct {
	Request TypedTableJoinArrangementRequest `json:"request"`
	Advice  TypedTableJoinArrangementAdvice  `json:"advice"`
}

// TypedTableJoinArrangementSelection is a deterministic, explainable choice.
// Selected is also the first entry in Candidates. Candidates are ordered by
// the same cost and safety rules used to select the winner.
type TypedTableJoinArrangementSelection struct {
	Selected   TypedTableJoinArrangementSelectionCandidate   `json:"selected"`
	Candidates []TypedTableJoinArrangementSelectionCandidate `json:"candidates"`
}

// SelectTypedTableJoinArrangement compares explicit join alternatives without
// changing any registry. It prefers the lowest incremental memory estimate,
// then lower transient and persistent memory, then the safest advisor action,
// then existing sharing/checkpoint quality, and finally the predicate key.
//
// Only exact definitions are compared. A caller remains responsible for
// acquiring, hydrating, and releasing the selected arrangement.
func SelectTypedTableJoinArrangement(
	catalog []TypedTableJoinArrangementInfo,
	request TypedTableJoinArrangementSelectionRequest,
	options TypedTableArrangementAdvisorOptions,
) (TypedTableJoinArrangementSelection, error) {
	if len(request.Alternatives) == 0 {
		return TypedTableJoinArrangementSelection{}, ErrTypedTableJoinArrangementSelectionNoCandidates
	}

	first := request.Alternatives[0]
	if len(request.Alternatives) == 1 {
		alternative := request.Alternatives[0]
		candidate := TypedTableJoinArrangementSelectionCandidate{
			Request: alternative,
			Advice:  AdviseTypedTableJoinArrangement(catalog, alternative, options),
		}
		return TypedTableJoinArrangementSelection{
			Selected:   candidate,
			Candidates: []TypedTableJoinArrangementSelectionCandidate{candidate},
		}, nil
	}

	candidates := make([]TypedTableJoinArrangementSelectionCandidate, 0, len(request.Alternatives))
	for index, alternative := range request.Alternatives {
		if alternative.LeftTableName != first.LeftTableName || alternative.RightTableName != first.RightTableName {
			return TypedTableJoinArrangementSelection{}, ErrTypedTableJoinArrangementSelectionTableMismatch
		}
		for previous := 0; previous < index; previous++ {
			if alternative.Definition == request.Alternatives[previous].Definition {
				return TypedTableJoinArrangementSelection{}, ErrTypedTableJoinArrangementSelectionDuplicate
			}
		}
		candidates = append(candidates, TypedTableJoinArrangementSelectionCandidate{
			Request: alternative,
			Advice:  AdviseTypedTableJoinArrangement(catalog, alternative, options),
		})
	}

	sort.Slice(candidates, func(left, right int) bool {
		return lessTypedTableJoinArrangementSelectionCandidate(candidates[left], candidates[right])
	})
	return TypedTableJoinArrangementSelection{
		Selected:   candidates[0],
		Candidates: candidates,
	}, nil
}

func lessTypedTableJoinArrangementSelectionCandidate(
	left, right TypedTableJoinArrangementSelectionCandidate,
) bool {
	leftMemory := left.Advice.Memory
	rightMemory := right.Advice.Memory
	if leftMemory.TotalBytes != rightMemory.TotalBytes {
		return leftMemory.TotalBytes < rightMemory.TotalBytes
	}
	if leftMemory.TransientBytes != rightMemory.TransientBytes {
		return leftMemory.TransientBytes < rightMemory.TransientBytes
	}
	if leftMemory.PersistentBytes != rightMemory.PersistentBytes {
		return leftMemory.PersistentBytes < rightMemory.PersistentBytes
	}
	if leftRank, rightRank := typedTableArrangementAdvisorActionRank(left.Advice.Action), typedTableArrangementAdvisorActionRank(right.Advice.Action); leftRank != rightRank {
		return leftRank < rightRank
	}

	leftRecommendation, leftHasRecommendation := typedTableJoinArrangementSelectionRecommendation(left.Advice)
	rightRecommendation, rightHasRecommendation := typedTableJoinArrangementSelectionRecommendation(right.Advice)
	if leftHasRecommendation && rightHasRecommendation {
		if leftRecommendation.References != rightRecommendation.References {
			return leftRecommendation.References > rightRecommendation.References
		}
		if leftRecommendation.LeftCheckpoint != rightRecommendation.LeftCheckpoint {
			return leftRecommendation.LeftCheckpoint > rightRecommendation.LeftCheckpoint
		}
		if leftRecommendation.RightCheckpoint != rightRecommendation.RightCheckpoint {
			return leftRecommendation.RightCheckpoint > rightRecommendation.RightCheckpoint
		}
	}
	if left.Request.EstimatedStateRows != right.Request.EstimatedStateRows {
		return left.Request.EstimatedStateRows < right.Request.EstimatedStateRows
	}
	return typedTableArrangementAdvisorJoinDefinitionKey(left.Request.Definition) < typedTableArrangementAdvisorJoinDefinitionKey(right.Request.Definition)
}

func typedTableJoinArrangementSelectionRecommendation(
	advice TypedTableJoinArrangementAdvice,
) (TypedTableJoinArrangementRecommendation, bool) {
	if len(advice.Candidates) == 0 {
		return TypedTableJoinArrangementRecommendation{}, false
	}
	return advice.Candidates[0], true
}
