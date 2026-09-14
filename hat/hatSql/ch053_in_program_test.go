package hatSql

import "testing"

func TestCH053InLiteralSemantics(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (1, 3, 5, NULL) SELECT src.id`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.where.inProgram == nil || len(query.where.inProgram.values) != 4 {
		t.Fatal("literal IN set was not prepared during query binding")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"id": int64(3)})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("IN matched value = %#v, want true", value)
	}
	row = newSQLSingleSourceExecRow("src", SQLRow{"id": int64(4)})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != nil {
		t.Fatalf("IN unknown value = %#v, want nil", value)
	}

	notIn, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id NOT IN (1, 3, 5, NULL) SELECT src.id`)
	if err != nil {
		t.Fatalf("parseSQLQuery(NOT IN) error = %v", err)
	}
	if value := evalSQLExpr(notIn.where, []sqlExecRow{row}, row); value != nil {
		t.Fatalf("NOT IN unknown value = %#v, want nil", value)
	}
}

func TestCH053InDynamicFallback(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (src.other, 3) SELECT src.id`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.where.inProgram != nil {
		t.Fatal("dynamic IN set unexpectedly received a literal program")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"id": int64(7), "other": int64(7)})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("dynamic IN = %#v, want true", value)
	}
}

func TestCH053InParameterPreparation(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.id IN ($1, 3) SELECT src.id`, []interface{}{int64(7)})
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	if query.where.inProgram == nil || len(query.where.inProgram.values) != 2 {
		t.Fatal("bound literal IN set was not prepared")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"id": int64(7)})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("bound IN = %#v, want true", value)
	}
}

func BenchmarkCH053PreparedLiteralInEvaluation(b *testing.B) {
	row := newSQLSingleSourceExecRow("src", SQLRow{"id": int64(31), "key": "key-31"})
	group := []sqlExecRow{row}

	b.Run("literal_8_numeric", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (1, 3, 5, 7, 9, 11, 13, 31) SELECT src.id`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.where
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value != true {
				b.Fatalf("literal numeric IN = %#v, want true", value)
			}
		}
	})

	b.Run("literal_32_numeric", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32) SELECT src.id`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.where
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value != true {
				b.Fatalf("literal numeric IN = %#v, want true", value)
			}
		}
	})

	b.Run("dynamic_mixed", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (src.other, 3, 5, 7, 9, 11, 13, 31) SELECT src.id`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.where
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value != true {
				b.Fatalf("dynamic IN = %#v, want true", value)
			}
		}
	})

	b.Run("literal_8_string", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.key IN ('key-1', 'key-3', 'key-5', 'key-7', 'key-9', 'key-11', 'key-13', 'key-31') SELECT src.key`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.where
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value != true {
				b.Fatalf("literal string IN = %#v, want true", value)
			}
		}
	})
}
