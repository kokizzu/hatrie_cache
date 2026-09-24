package hatSql

import "testing"

func TestCH041GroupingBranchCloneCopiesRewriteState(t *testing.T) {
	from := &sqlSource{kind: "VALUES", alias: "src"}
	source := &sqlQuery{
		ctes:               []sqlCTE{{name: "source"}},
		from:               from,
		joins:              []sqlJoin{{kind: "INNER", source: sqlSource{kind: "CACHE", alias: "lookup"}, on: sqlExpr{kind: "literal", value: true}}},
		where:              sqlExpr{kind: "field", qualifier: "src", name: "active"},
		prewhere:           sqlExpr{kind: "literal", value: true},
		selects:            []sqlSelectItem{{expr: sqlExpr{kind: "field", qualifier: "src", name: "region"}, alias: "region"}},
		groupBy:            []sqlExpr{{kind: "field", qualifier: "src", name: "region"}},
		groupingSets:       [][]sqlExpr{{{kind: "field", qualifier: "src", name: "region"}}, nil},
		groupingDimensions: []sqlExpr{{kind: "field", qualifier: "src", name: "region"}},
		having:             sqlExpr{kind: "field", qualifier: "src", name: "total"},
		orderBy:            []sqlOrder{{expr: sqlExpr{kind: "field", qualifier: "src", name: "region"}}},
		limitBy:            &sqlLimitBy{limit: 2, expressions: []sqlExpr{{kind: "field", qualifier: "src", name: "region"}}},
	}

	branch := cloneSQLGroupingSetBranch(source)
	if branch == nil {
		t.Fatal("cloneSQLGroupingSetBranch returned nil")
	}
	if branch.from != source.from {
		t.Fatal("grouping branch should share the immutable source descriptor")
	}
	if len(branch.joins) != 1 || &branch.joins[0] != &source.joins[0] {
		t.Fatal("grouping branch should share immutable join descriptors")
	}
	if len(branch.ctes) != 1 || &branch.ctes[0] != &source.ctes[0] {
		t.Fatal("grouping branch should share immutable CTE descriptors")
	}
	if branch.groupingSets != nil || branch.groupingDimensions != nil || branch.unions != nil {
		t.Fatal("grouping branch should clear expansion-only state")
	}

	branch.selects[0].expr.name = "changed"
	branch.groupBy[0].name = "changed"
	branch.having.name = "changed"
	branch.orderBy[0].expr.name = "changed"
	branch.limitBy.expressions[0].name = "changed"
	if source.selects[0].expr.name != "region" || source.groupBy[0].name != "region" || source.having.name != "total" || source.orderBy[0].expr.name != "region" || source.limitBy.expressions[0].name != "region" {
		t.Fatal("grouping branch rewrites leaked into the template")
	}
}

func TestCH041GroupingBranchClonePreservesGroupingOutputRewrite(t *testing.T) {
	source := &sqlQuery{
		selects:            []sqlSelectItem{{expr: sqlExpr{kind: "field", qualifier: "src", name: "region"}}},
		groupingDimensions: []sqlExpr{{kind: "field", qualifier: "src", name: "region"}},
	}
	branch := cloneSQLGroupingSetBranch(source)
	sqlNullAbsentGroupingDimensions(branch, nil, source.groupingDimensions)
	if source.selects[0].expr.kind != "field" {
		t.Fatal("absent-dimension rewrite changed the template")
	}
	if branch.selects[0].expr.kind != "literal" || branch.selects[0].expr.value != nil {
		t.Fatalf("branch select = %#v, want NULL literal", branch.selects[0].expr)
	}
}

var benchmarkCH041GroupingBranchSink *sqlQuery

func benchmarkCH041GroupingBranchQuery() *sqlQuery {
	values := make([][]interface{}, 128)
	for index := range values {
		values[index] = []interface{}{index, index % 8, index % 4, index%2 == 0}
	}
	region := sqlExpr{kind: "field", qualifier: "src", name: "region"}
	product := sqlExpr{kind: "field", qualifier: "src", name: "product"}
	amount := sqlExpr{kind: "field", qualifier: "src", name: "amount"}
	return &sqlQuery{
		ctes:               []sqlCTE{{name: "source", values: values, columns: []string{"id", "region", "product", "active"}}},
		from:               &sqlSource{kind: "VALUES", alias: "src", values: values, columns: []string{"id", "region", "product", "active"}},
		joins:              []sqlJoin{{kind: "INNER", source: sqlSource{kind: "CACHE", alias: "lookup"}, on: sqlExpr{kind: "literal", value: true}}},
		where:              sqlExpr{kind: "field", qualifier: "src", name: "active"},
		selects:            []sqlSelectItem{{expr: region}, {expr: product}, {expr: amount}},
		groupBy:            []sqlExpr{region, product},
		groupingSets:       [][]sqlExpr{{region, product}, {region}, nil},
		groupingDimensions: []sqlExpr{region, product},
		having:             sqlExpr{kind: "field", qualifier: "src", name: "amount"},
		orderBy:            []sqlOrder{{expr: region}, {expr: product}},
		limitBy:            &sqlLimitBy{limit: 2, expressions: []sqlExpr{region}},
	}
}

func BenchmarkCH041GroupingBranchClone(b *testing.B) {
	source := benchmarkCH041GroupingBranchQuery()
	b.Run("deep_query_clone", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			benchmarkCH041GroupingBranchSink = cloneSQLQuery(source)
		}
	})
	b.Run("shared_grouping_clone", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			benchmarkCH041GroupingBranchSink = cloneSQLGroupingSetBranch(source)
		}
	})
}
