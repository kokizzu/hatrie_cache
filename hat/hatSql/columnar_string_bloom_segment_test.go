package hatSql

import "testing"

func TestColumnarStringBloomSegmentHasNoFalseNegatives(t *testing.T) {
	t.Parallel()
	values := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	segment := ColumnarStringBloomSegment{}
	for _, value := range values {
		segment.Add(value)
	}
	for _, value := range values {
		if !segment.MayContain(value) {
			t.Fatalf("MayContain(%q) = false after Add", value)
		}
	}
}

func TestSQLColumnarStringEqualityPredicateRequiresBinaryCollation(t *testing.T) {
	t.Parallel()
	expr := sqlExpr{
		kind:      "binary",
		op:        "=",
		collation: SQLCollationUnicodeCI,
		left:      &sqlExpr{kind: "field", name: "tag"},
		right:     &sqlExpr{kind: "literal", value: "alpha"},
	}
	if _, _, ok := sqlColumnarStringEqualityPredicate(expr, "event"); ok {
		t.Fatal("Unicode collation unexpectedly selected binary string equality fast path")
	}
}
