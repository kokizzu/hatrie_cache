package hatSql

import (
	"encoding/json"
	"fmt"
	"strings"
)

const explainDataflowFormat = "hatrie-cache-explain-dataflow/v1"

// ExplainDataflowGraph is a versioned structural graph derived from EXPLAIN
// steps. It keeps the original plan order while making nested subplans and
// same-level pipeline edges explicit.
type ExplainDataflowGraph struct {
	Format string                `json:"format"`
	Nodes  []ExplainDataflowNode `json:"nodes"`
	Edges  []ExplainDataflowEdge `json:"edges"`
}

// ExplainDataflowNode is one independent copy of an ExplainStep. Depth is
// derived from the two-space indentation used by the SQL explain builder.
type ExplainDataflowNode struct {
	ID    string      `json:"id"`
	Depth int         `json:"depth"`
	Step  ExplainStep `json:"step"`
}

// ExplainDataflowEdge connects operators in the structural graph. Pipeline
// edges connect consecutive operators at one depth; subplan edges connect a
// nested operator to the nearest preceding parent operator.
type ExplainDataflowEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Kind string `json:"kind"`
}

// BuildExplainDataflowGraph returns a deterministic, independent graph for
// steps produced by EXPLAIN or EXPLAIN ANALYZE. It does not execute a query or
// change the existing ExplainStep representation.
func BuildExplainDataflowGraph(steps []ExplainStep) ExplainDataflowGraph {
	graph := ExplainDataflowGraph{
		Format: explainDataflowFormat,
		Nodes:  make([]ExplainDataflowNode, 0, len(steps)),
		Edges:  make([]ExplainDataflowEdge, 0, len(steps)),
	}
	lastAtDepth := make(map[int]int)
	maxDepth := 0
	for index, source := range steps {
		depth := explainDataflowStepDepth(source.Node)
		for level := depth + 1; level <= maxDepth; level++ {
			delete(lastAtDepth, level)
		}
		if depth > maxDepth {
			maxDepth = depth
		}
		nodeIndex := len(graph.Nodes)
		nodeID := fmt.Sprintf("op%d", index)
		step := cloneExplainDataflowStep(source)
		step.Node = strings.TrimLeft(step.Node, " \t")
		graph.Nodes = append(graph.Nodes, ExplainDataflowNode{ID: nodeID, Depth: depth, Step: step})

		if previous, exists := lastAtDepth[depth]; exists {
			graph.Edges = append(graph.Edges, ExplainDataflowEdge{From: graph.Nodes[previous].ID, To: nodeID, Kind: "pipeline"})
		} else if depth > 0 {
			parent := -1
			for level := depth - 1; level >= 0; level-- {
				if previous, exists := lastAtDepth[level]; exists {
					parent = previous
					break
				}
			}
			if parent >= 0 {
				graph.Edges = append(graph.Edges, ExplainDataflowEdge{From: graph.Nodes[parent].ID, To: nodeID, Kind: "subplan"})
			}
		}
		lastAtDepth[depth] = nodeIndex
	}
	return graph
}

// MarshalExplainDataflowJSON encodes a versioned structural EXPLAIN graph.
func MarshalExplainDataflowJSON(steps []ExplainStep) ([]byte, error) {
	return json.Marshal(BuildExplainDataflowGraph(steps))
}

// ExplainDataflowDOT renders the structural EXPLAIN graph as Graphviz DOT.
// Operator IDs are generated locally, so plan details are emitted only as
// escaped labels.
func ExplainDataflowDOT(steps []ExplainStep) string {
	graph := BuildExplainDataflowGraph(steps)
	var builder strings.Builder
	builder.WriteString("digraph hatrie_cache_explain_dataflow {\n  rankdir=LR;\n")
	for _, node := range graph.Nodes {
		label := node.Step.Node
		if node.Step.Detail != "" {
			label += "\\n" + node.Step.Detail
		}
		fmt.Fprintf(&builder, "  %s [shape=box,label=%q];\n", node.ID, label)
	}
	for _, edge := range graph.Edges {
		fmt.Fprintf(&builder, "  %s -> %s [label=%q];\n", edge.From, edge.To, edge.Kind)
	}
	builder.WriteString("}\n")
	return builder.String()
}

func explainDataflowStepDepth(node string) int {
	indentation := 0
	for _, character := range node {
		switch character {
		case ' ':
			indentation++
		case '\t':
			indentation += 2
		default:
			return indentation / 2
		}
	}
	return indentation / 2
}

func cloneExplainDataflowStep(step ExplainStep) ExplainStep {
	clone := step
	if step.Lineage != nil {
		clone.Lineage = make([]ColumnLineage, len(step.Lineage))
		for index, lineage := range step.Lineage {
			clone.Lineage[index] = lineage
			clone.Lineage[index].SourceFields = append([]string(nil), lineage.SourceFields...)
		}
	}
	clone.EstimatedRows = cloneExplainDataflowInt(step.EstimatedRows)
	clone.ActualInputRows = cloneExplainDataflowInt(step.ActualInputRows)
	clone.ActualOutputRows = cloneExplainDataflowInt(step.ActualOutputRows)
	clone.ActualInputBytes = cloneExplainDataflowInt(step.ActualInputBytes)
	clone.ActualOutputBytes = cloneExplainDataflowInt(step.ActualOutputBytes)
	clone.EstimateErrorRows = cloneExplainDataflowInt(step.EstimateErrorRows)
	clone.EstimateErrorPercent = cloneExplainDataflowFloat(step.EstimateErrorPercent)
	clone.ElapsedNanos = cloneExplainDataflowInt64(step.ElapsedNanos)
	return clone
}

func cloneExplainDataflowInt(value *int) *int {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneExplainDataflowFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneExplainDataflowInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
