package hatSql

import (
	"strings"
	"testing"
)

func TestMU028SQLExpressionMonotonicityConservativeAlgebra(t *testing.T) {
	tests := []struct {
		expression    string
		column        string
		wantDirection SQLMonotonicity
		wantDepends   bool
		wantNullable  bool
	}{
		{expression: "price", column: "price", wantDirection: SQLMonotonicityNonDecreasing, wantDepends: true, wantNullable: true},
		{expression: "price + 10", column: "price", wantDirection: SQLMonotonicityNonDecreasing, wantDepends: true, wantNullable: true},
		{expression: "10 - price", column: "price", wantDirection: SQLMonotonicityNonIncreasing, wantDepends: true, wantNullable: true},
		{expression: "price * -2", column: "price", wantDirection: SQLMonotonicityNonIncreasing, wantDepends: true, wantNullable: true},
		{expression: "price * 0", column: "price", wantDirection: SQLMonotonicityConstant, wantDepends: false, wantNullable: true},
		{expression: "-price", column: "price", wantDirection: SQLMonotonicityNonIncreasing, wantDepends: true, wantNullable: true},
		{expression: "price + NULL", column: "price", wantDirection: SQLMonotonicityConstant, wantDepends: false, wantNullable: true},
		{expression: "price + (NULL + 1)", column: "price", wantDirection: SQLMonotonicityConstant, wantDepends: false, wantNullable: true},
		{expression: "price >= 10", column: "price", wantDirection: SQLMonotonicityNonDecreasing, wantDepends: true, wantNullable: true},
		{expression: "price <= 10", column: "price", wantDirection: SQLMonotonicityNonIncreasing, wantDepends: true, wantNullable: true},
		{expression: "NOT (price >= 10)", column: "price", wantDirection: SQLMonotonicityNonIncreasing, wantDepends: true, wantNullable: true},
		{expression: "price >= 10 AND TRUE", column: "price", wantDirection: SQLMonotonicityNonDecreasing, wantDepends: true, wantNullable: true},
		{expression: "price = 10", column: "price", wantDirection: SQLMonotonicityUnknown, wantDepends: true, wantNullable: true},
		{expression: "price >= NULL", column: "price", wantDirection: SQLMonotonicityConstant, wantDepends: false, wantNullable: true},
		{expression: "price / 0", column: "price", wantDirection: SQLMonotonicityUnknown, wantDepends: true, wantNullable: true},
		{expression: "price + other", column: "price", wantDirection: SQLMonotonicityUnknown, wantDepends: true, wantNullable: true},
		{expression: "abs(price)", column: "price", wantDirection: SQLMonotonicityUnknown, wantDepends: true, wantNullable: true},
		{expression: "COUNT(*) FILTER (WHERE price >= 1)", column: "price", wantDirection: SQLMonotonicityUnknown, wantDepends: true, wantNullable: true},
		{expression: "NULL", column: "price", wantDirection: SQLMonotonicityConstant, wantDepends: false, wantNullable: true},
		{expression: "other", column: "price", wantDirection: SQLMonotonicityUnknown, wantDepends: false, wantNullable: true},
	}
	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			report, err := AnalyzeSQLExpressionMonotonicity(test.expression, test.column)
			if err != nil {
				t.Fatal(err)
			}
			if report.Monotonicity != test.wantDirection {
				t.Fatalf("direction = %s, want %s; reason=%q", report.Monotonicity, test.wantDirection, report.Reason)
			}
			if report.DependsOnColumn != test.wantDepends {
				t.Fatalf("depends on column = %v, want %v", report.DependsOnColumn, test.wantDepends)
			}
			if report.MayReturnNull != test.wantNullable {
				t.Fatalf("may return NULL = %v, want %v", report.MayReturnNull, test.wantNullable)
			}
			if strings.TrimSpace(report.Reason) == "" {
				t.Fatal("reason is empty")
			}
		})
	}
}

func TestMU028SQLExpressionMonotonicityQualifiedColumnAndErrors(t *testing.T) {
	report, err := AnalyzeSQLExpressionMonotonicity("orders.price + 1", "orders.price")
	if err != nil {
		t.Fatal(err)
	}
	if report.Monotonicity != SQLMonotonicityNonDecreasing || !report.DependsOnColumn {
		t.Fatalf("qualified report = %+v", report)
	}

	for _, test := range []struct {
		name       string
		expression string
		column     string
	}{
		{name: "empty expression", expression: "", column: "price"},
		{name: "empty column", expression: "price", column: ""},
		{name: "trailing tokens", expression: "price + 1 price", column: "price"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := AnalyzeSQLExpressionMonotonicity(test.expression, test.column); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestMU028SQLMonotonicityStringNames(t *testing.T) {
	for _, test := range []struct {
		value SQLMonotonicity
		want  string
	}{
		{value: SQLMonotonicityUnknown, want: "unknown"},
		{value: SQLMonotonicityConstant, want: "constant"},
		{value: SQLMonotonicityNonDecreasing, want: "non-decreasing"},
		{value: SQLMonotonicityNonIncreasing, want: "non-increasing"},
	} {
		if got := test.value.String(); got != test.want {
			t.Errorf("String() = %q, want %q", got, test.want)
		}
	}
}
