package hatSql

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ExplainDocument is the stable machine-readable representation of one SQL
// plan. Steps retain the same fields returned by EXPLAIN and EXPLAIN ANALYZE.
type ExplainDocument struct {
	Format string        `json:"format"`
	Steps  []ExplainStep `json:"steps"`
}

// MarshalExplainJSON encodes a versioned SQL plan for tools and diagnostics.
func MarshalExplainJSON(steps []ExplainStep) ([]byte, error) {
	return json.Marshal(ExplainDocument{Format: "hatrie-cache-explain/v1", Steps: steps})
}

// ExplainDOT renders a plan as a simple directed operator graph compatible
// with Graphviz DOT. Plan detail is escaped and never interpreted as DOT.
func ExplainDOT(steps []ExplainStep) string {
	var builder strings.Builder
	builder.WriteString("digraph hatrie_cache_explain {\n  rankdir=LR;\n")
	for index, step := range steps {
		label := step.Node
		if step.Detail != "" {
			label += "\\n" + step.Detail
		}
		fmt.Fprintf(&builder, "  op%d [shape=box,label=%q];\n", index, label)
		if index > 0 {
			fmt.Fprintf(&builder, "  op%d -> op%d;\n", index-1, index)
		}
	}
	builder.WriteString("}\n")
	return builder.String()
}
