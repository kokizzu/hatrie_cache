package hatSql

import "fmt"

// SQLDataflowNode is one immutable logical stage in a compiled SQL dataflow.
// Inputs refer to earlier node IDs in the same SQLDataflowIR.
type SQLDataflowNode struct {
	ID     int    `json:"id"`
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
	Inputs []int  `json:"inputs,omitempty"`
}

// SQLDataflowIR is a stable, read-only description of a compiled query's
// logical stages. It is intended for routing, admission, explain tooling, and
// reusable plan registries; execution still uses CompiledSQLQuery's existing
// cloned-template path.
type SQLDataflowIR struct {
	Source string            `json:"source"`
	Nodes  []SQLDataflowNode `json:"nodes"`
	Root   int               `json:"root"`
}

// Dataflow returns a fresh logical IR snapshot. Mutating the returned value
// cannot mutate the compiled query or a later snapshot.
func (query *CompiledSQLQuery) Dataflow() SQLDataflowIR {
	if query == nil || query.template == nil {
		return SQLDataflowIR{Root: -1}
	}
	ir := SQLDataflowIR{Source: query.source, Root: -1}
	previous := -1
	appendNode := func(kind, detail string) {
		node := SQLDataflowNode{ID: len(ir.Nodes), Kind: kind, Detail: detail}
		if previous >= 0 {
			node.Inputs = []int{previous}
		}
		ir.Nodes = append(ir.Nodes, node)
		previous = node.ID
		ir.Root = node.ID
	}

	if query.template.from != nil {
		appendNode("SCAN", sqlExplainSource(*query.template.from))
	}
	for _, join := range query.template.joins {
		appendNode("JOIN", fmt.Sprintf("%s %s", join.kind, sqlExplainSource(join.source)))
	}
	if query.template.where.kind != "" {
		appendNode("FILTER", sqlExplainExpression(query.template.where))
	}
	if len(query.template.groupBy) > 0 || len(query.template.groupingSets) > 0 || len(query.template.groupingDimensions) > 0 {
		appendNode("AGGREGATE", sqlExplainExpressions(query.template.groupBy))
	}
	if query.template.having.kind != "" {
		appendNode("HAVING", sqlExplainExpression(query.template.having))
	}
	if len(query.template.selects) > 0 {
		appendNode("PROJECT", sqlExplainSelects(query.template.selects))
	}
	if query.template.distinct {
		appendNode("DISTINCT", "distinct rows")
	}
	if len(query.template.orderBy) > 0 {
		appendNode("SORT", fmt.Sprintf("%d order keys", len(query.template.orderBy)))
	}
	if query.template.limit >= 0 || query.template.offset > 0 {
		appendNode("LIMIT", fmt.Sprintf("limit=%d offset=%d", query.template.limit, query.template.offset))
	}
	for range query.template.unions {
		appendNode("UNION", "set branch")
	}
	return ir
}
