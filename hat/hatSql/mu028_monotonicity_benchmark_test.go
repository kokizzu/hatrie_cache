package hatSql

import "testing"

func BenchmarkMU028SQLExpressionMonotonicity(b *testing.B) {
	tests := []struct {
		name       string
		expression string
		column     string
	}{
		{name: "price_plus_constant", expression: "price + 10", column: "price"},
		{name: "price_threshold", expression: "price >= 10", column: "price"},
		{name: "unsupported_function", expression: "abs(price)", column: "price"},
	}
	for _, test := range tests {
		b.Run("baseline_parse/"+test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := parseMU028ExpressionBaseline(test.expression); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("analyze/"+test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := AnalyzeSQLExpressionMonotonicity(test.expression, test.column); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func parseMU028ExpressionBaseline(expression string) error {
	tokens, err := lexSQL(expression)
	if err != nil {
		return err
	}
	parser := sqlQueryParser{tokens: tokens}
	if _, err := parser.parseCondition(); err != nil {
		return err
	}
	if parser.current().kind == sqlTokenSemicolon {
		parser.next()
	}
	if parser.current().kind != sqlTokenEOF {
		return parser.expected(parser.current(), "end of input", nil)
	}
	return nil
}
