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

// SQLDataflowFragment is one reusable logical operator in a lowered SQL plan.
// Inputs refer to earlier fragment IDs in the same SQLDataflowPlan. A caller
// can execute validated fragments with CompileSQLDataflow and a runner-owned
// operator implementation; the existing SQL executor remains unchanged.
type SQLDataflowFragment struct {
	ID     int    `json:"id"`
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
	Inputs []int  `json:"inputs,omitempty"`
}

// SQLDataflowPlan is a versioned, reusable lowering of a compiled SQL query.
// It is safe to retain after LowerDataflow returns because every call returns
// independent slices and the compiled query keeps its own private plan.
type SQLDataflowPlan struct {
	Format    string                `json:"format"`
	Source    string                `json:"source"`
	Fragments []SQLDataflowFragment `json:"fragments"`
	Root      int                   `json:"root"`
}

const sqlDataflowPlanFormat = "hatrie-cache-sql-dataflow/v1"

// LowerDataflow returns a fresh reusable logical fragment plan. Mutating the
// returned value cannot mutate the compiled query or a later snapshot.
func (query *CompiledSQLQuery) LowerDataflow() SQLDataflowPlan {
	if query == nil || query.template == nil {
		return SQLDataflowPlan{Format: sqlDataflowPlanFormat, Root: -1}
	}
	query.dataflowOnce.Do(func() {
		query.dataflowPlan = buildSQLDataflowPlan(query.source, query.template)
	})
	return cloneSQLDataflowPlan(*query.dataflowPlan)
}

// Dataflow returns a fresh logical IR snapshot. Mutating the returned value
// cannot mutate the compiled query or a later snapshot.
func (query *CompiledSQLQuery) Dataflow() SQLDataflowIR {
	if query == nil || query.template == nil {
		return SQLDataflowIR{Root: -1}
	}
	plan := query.LowerDataflow()
	ir := SQLDataflowIR{Source: plan.Source, Root: plan.Root, Nodes: make([]SQLDataflowNode, len(plan.Fragments))}
	for index, fragment := range plan.Fragments {
		ir.Nodes[index] = SQLDataflowNode{
			ID:     fragment.ID,
			Kind:   fragment.Kind,
			Detail: fragment.Detail,
			Inputs: append([]int(nil), fragment.Inputs...),
		}
	}
	return ir
}

func buildSQLDataflowPlan(source string, template *sqlQuery) *SQLDataflowPlan {
	plan := &SQLDataflowPlan{Format: sqlDataflowPlanFormat, Source: source, Root: -1}
	previous := -1
	appendFragment := func(kind, detail string) {
		fragment := SQLDataflowFragment{ID: len(plan.Fragments), Kind: kind, Detail: detail}
		if previous >= 0 {
			fragment.Inputs = []int{previous}
		}
		plan.Fragments = append(plan.Fragments, fragment)
		previous = fragment.ID
		plan.Root = fragment.ID
	}

	if template.from != nil {
		appendFragment("SCAN", sqlExplainSource(*template.from))
	}
	for _, join := range template.joins {
		appendFragment("JOIN", fmt.Sprintf("%s %s", join.kind, sqlExplainSource(join.source)))
	}
	if template.where.kind != "" {
		appendFragment("FILTER", sqlExplainExpression(template.where))
	}
	if len(template.groupBy) > 0 || len(template.groupingSets) > 0 || len(template.groupingDimensions) > 0 {
		appendFragment("AGGREGATE", sqlExplainExpressions(template.groupBy))
	}
	if template.having.kind != "" {
		appendFragment("HAVING", sqlExplainExpression(template.having))
	}
	if len(template.selects) > 0 {
		appendFragment("PROJECT", sqlExplainSelects(template.selects))
	}
	if template.distinct {
		appendFragment("DISTINCT", "distinct rows")
	}
	if len(template.orderBy) > 0 {
		appendFragment("SORT", fmt.Sprintf("%d order keys", len(template.orderBy)))
	}
	if template.limit >= 0 || template.offset > 0 {
		appendFragment("LIMIT", fmt.Sprintf("limit=%d offset=%d", template.limit, template.offset))
	}
	for range template.unions {
		appendFragment("UNION", "set branch")
	}
	return plan
}

func cloneSQLDataflowPlan(plan SQLDataflowPlan) SQLDataflowPlan {
	clone := plan
	clone.Fragments = make([]SQLDataflowFragment, len(plan.Fragments))
	for index, fragment := range plan.Fragments {
		clone.Fragments[index] = fragment
		clone.Fragments[index].Inputs = append([]int(nil), fragment.Inputs...)
	}
	return clone
}
