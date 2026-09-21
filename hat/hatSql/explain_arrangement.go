package hatSql

import (
	"encoding/json"
	"fmt"
	"strings"
)

const explainArrangementFormat = "hatrie-cache-explain-arrangements/v1"

// MaxExplainArrangementAnnotations bounds the number of operator annotations
// accepted in one enriched explain document.
const MaxExplainArrangementAnnotations = 4096

// MaxDataflowTextBytes bounds one arrangement annotation key.
const MaxDataflowTextBytes = 1 << 20

// ExplainArrangementMetadata describes the arrangement decision attached to
// one explain operator. EstimatedBytes is an incremental memory estimate from
// the arrangement advisor, not an allocator measurement.
type ExplainArrangementMetadata struct {
	Key            string                             `json:"key"`
	Action         TypedTableArrangementAdvisorAction `json:"action"`
	References     int                                `json:"references,omitempty"`
	Shared         bool                               `json:"shared,omitempty"`
	Stale          bool                               `json:"stale,omitempty"`
	EstimatedBytes uint64                             `json:"estimated_bytes,omitempty"`
}

// ExplainArrangementAnnotation attaches arrangement metadata to one step by
// its stable zero-based position in an existing ExplainStep plan.
type ExplainArrangementAnnotation struct {
	StepIndex      int
	Key            string
	Action         TypedTableArrangementAdvisorAction
	References     int
	Shared         bool
	Stale          bool
	EstimatedBytes uint64
}

// ExplainArrangementStep keeps the original ExplainStep intact while adding
// optional arrangement metadata. Existing explain consumers can continue to
// use ExplainStep plans without changing behavior.
type ExplainArrangementStep struct {
	Step        ExplainStep                 `json:"step"`
	Arrangement *ExplainArrangementMetadata `json:"arrangement,omitempty"`
}

// ExplainArrangementPlan is a versioned, deterministic explain document with
// arrangement keys, reuse decisions, cardinality from ExplainStep, and
// incremental memory estimates.
type ExplainArrangementPlan struct {
	Format string                   `json:"format"`
	Steps  []ExplainArrangementStep `json:"steps"`
}

// BuildExplainArrangementPlan clones steps and attaches validated annotations.
// It does not execute a query, access a resolver, or mutate the input plan.
func BuildExplainArrangementPlan(steps []ExplainStep, annotations []ExplainArrangementAnnotation) (ExplainArrangementPlan, error) {
	if len(annotations) > MaxExplainArrangementAnnotations {
		return ExplainArrangementPlan{}, fmt.Errorf("too many explain arrangement annotations: %d", len(annotations))
	}
	plan := ExplainArrangementPlan{
		Format: explainArrangementFormat,
		Steps:  make([]ExplainArrangementStep, len(steps)),
	}
	for index, step := range steps {
		plan.Steps[index].Step = cloneExplainArrangementStep(step)
	}
	seen := make(map[int]struct{}, len(annotations))
	for _, annotation := range annotations {
		if annotation.StepIndex < 0 || annotation.StepIndex >= len(plan.Steps) {
			return ExplainArrangementPlan{}, fmt.Errorf("explain arrangement annotation step index %d is out of range", annotation.StepIndex)
		}
		if _, exists := seen[annotation.StepIndex]; exists {
			return ExplainArrangementPlan{}, fmt.Errorf("duplicate explain arrangement annotation for step %d", annotation.StepIndex)
		}
		seen[annotation.StepIndex] = struct{}{}
		key := strings.TrimSpace(annotation.Key)
		if key == "" {
			return ExplainArrangementPlan{}, fmt.Errorf("explain arrangement annotation key is empty for step %d", annotation.StepIndex)
		}
		if len(key) > MaxDataflowTextBytes {
			return ExplainArrangementPlan{}, fmt.Errorf("explain arrangement annotation key exceeds %d bytes", MaxDataflowTextBytes)
		}
		if annotation.References < 0 {
			return ExplainArrangementPlan{}, fmt.Errorf("explain arrangement annotation references cannot be negative")
		}
		if !validExplainArrangementAction(annotation.Action) {
			return ExplainArrangementPlan{}, fmt.Errorf("unknown explain arrangement action %q", annotation.Action)
		}
		plan.Steps[annotation.StepIndex].Arrangement = &ExplainArrangementMetadata{
			Key:            key,
			Action:         annotation.Action,
			References:     annotation.References,
			Shared:         annotation.Shared || annotation.References > 1,
			Stale:          annotation.Stale,
			EstimatedBytes: annotation.EstimatedBytes,
		}
	}
	return plan, nil
}

// MarshalExplainArrangementJSON encodes an enriched explain document in a
// stable versioned format.
func MarshalExplainArrangementJSON(steps []ExplainStep, annotations []ExplainArrangementAnnotation) ([]byte, error) {
	plan, err := BuildExplainArrangementPlan(steps, annotations)
	if err != nil {
		return nil, err
	}
	return json.Marshal(plan)
}

// ExplainArrangementDOT renders an enriched explain plan as Graphviz DOT.
// Arrangement metadata is emitted only as escaped labels.
func ExplainArrangementDOT(plan ExplainArrangementPlan) string {
	var builder strings.Builder
	builder.WriteString("digraph hatrie_cache_explain_arrangements {\n  rankdir=LR;\n")
	for index, step := range plan.Steps {
		label := step.Step.Node
		if step.Step.Detail != "" {
			label += "\\n" + step.Step.Detail
		}
		if step.Arrangement != nil {
			label += "\\narrangement=" + step.Arrangement.Key + " action=" + string(step.Arrangement.Action)
			if step.Arrangement.Stale {
				label += " stale"
			}
		}
		fmt.Fprintf(&builder, "  op%d [shape=box,label=%q];\n", index, label)
		if index > 0 {
			fmt.Fprintf(&builder, "  op%d -> op%d;\n", index-1, index)
		}
	}
	builder.WriteString("}\n")
	return builder.String()
}

func validExplainArrangementAction(action TypedTableArrangementAdvisorAction) bool {
	switch action {
	case TypedTableArrangementAdvisorCreate, TypedTableArrangementAdvisorReuse, TypedTableArrangementAdvisorHydrateThenReuse:
		return true
	default:
		return false
	}
}

func cloneExplainArrangementStep(step ExplainStep) ExplainStep {
	clone := cloneExplainDataflowStep(step)
	if step.Alternatives != nil {
		clone.Alternatives = append([]ExplainAlternative(nil), step.Alternatives...)
	}
	if step.Notices != nil {
		clone.Notices = append([]ExplainNotice(nil), step.Notices...)
	}
	return clone
}
