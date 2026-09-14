package hatSql

import "testing"

func TestCH054InSearchSemantics(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (1, 3, 5, 31) SELECT src.id`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.where.inProgram == nil || query.where.inProgram.mode != sqlInProgramLinear {
		t.Fatalf("small numeric IN mode = %v, want linear", query.where.inProgram)
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"id": int64(31)})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("numeric IN hit = %#v, want true", value)
	}
	row = newSQLSingleSourceExecRow("src", SQLRow{"id": int64(99)})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != false {
		t.Fatalf("numeric IN miss = %#v, want false", value)
	}

	query, err = parseSQLQuery(`FROM CACHE('events') AS src WHERE src.key IN ('key-1', 'key-3', 'key-31') SELECT src.key`)
	if err != nil {
		t.Fatalf("parseSQLQuery(string) error = %v", err)
	}
	if query.where.inProgram == nil || query.where.inProgram.mode != sqlInProgramLinear {
		t.Fatalf("small string IN mode = %v, want linear", query.where.inProgram)
	}
	row = newSQLSingleSourceExecRow("src", SQLRow{"key": "key-31"})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("string IN hit = %#v, want true", value)
	}

	query, err = parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (1, 2, 3, 4, 5, 6, 7, 8) SELECT src.id`)
	if err != nil {
		t.Fatalf("parseSQLQuery(search) error = %v", err)
	}
	if query.where.inProgram == nil || query.where.inProgram.mode != sqlInProgramNumericSearch {
		t.Fatalf("numeric search mode = %v, want numeric search", query.where.inProgram)
	}
	row = newSQLSingleSourceExecRow("src", SQLRow{"id": float64(8)})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("cross-type numeric IN = %#v, want true", value)
	}
	notIn, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id NOT IN (1, 2, 3, 4, 5, 6, 7, 8) SELECT src.id`)
	if err != nil {
		t.Fatalf("parseSQLQuery(NOT IN search) error = %v", err)
	}
	row = newSQLSingleSourceExecRow("src", SQLRow{"id": int64(99)})
	if value := evalSQLExpr(notIn.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("numeric NOT IN miss = %#v, want true", value)
	}
	row = newSQLSingleSourceExecRow("src", SQLRow{"id": int64(8)})
	if value := evalSQLExpr(notIn.where, []sqlExecRow{row}, row); value != false {
		t.Fatalf("numeric NOT IN hit = %#v, want false", value)
	}

	query, err = parseSQLQuery(`FROM CACHE('events') AS src WHERE src.key IN ('key-1', 'key-3', 'key-5', 'key-7', 'key-9', 'key-11', 'key-13', 'key-31') SELECT src.key`)
	if err != nil {
		t.Fatalf("parseSQLQuery(string search) error = %v", err)
	}
	if query.where.inProgram == nil || query.where.inProgram.mode != sqlInProgramStringSearch {
		t.Fatalf("string search mode = %v, want string search", query.where.inProgram)
	}
	applySQLQueryCollation(query, SQLCollationUnicodeCI)
	row = newSQLSingleSourceExecRow("src", SQLRow{"key": "KEY-31"})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("Unicode collation IN = %#v, want true", value)
	}
}

func TestCH054InSearchFallbackSemantics(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (src.other, 3, 5, 31) SELECT src.id`)
	if err != nil {
		t.Fatalf("parseSQLQuery(dynamic) error = %v", err)
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"id": int64(31), "other": int64(7)})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("dynamic IN = %#v, want true", value)
	}

	query, err = parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (1, NULL, 5, 31) SELECT src.id`)
	if err != nil {
		t.Fatalf("parseSQLQuery(NULL) error = %v", err)
	}
	if query.where.inProgram == nil || query.where.inProgram.mode != sqlInProgramLinear {
		t.Fatalf("NULL IN mode = %v, want linear", query.where.inProgram)
	}
	row = newSQLSingleSourceExecRow("src", SQLRow{"id": int64(99)})
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != nil {
		t.Fatalf("NULL IN miss = %#v, want nil", value)
	}
}

func BenchmarkCH054InSearchEvaluation(b *testing.B) {
	row := newSQLSingleSourceExecRow("src", SQLRow{"id": int64(31), "key": "key-31"})
	group := []sqlExecRow{row}

	b.Run("numeric_8_hit", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (1, 3, 5, 7, 9, 11, 13, 31) SELECT src.id`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.where
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value != true {
				b.Fatalf("numeric IN = %#v, want true", value)
			}
		}
	})

	b.Run("numeric_32_hit", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32) SELECT src.id`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.where
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value != true {
				b.Fatalf("numeric IN = %#v, want true", value)
			}
		}
	})

	b.Run("numeric_32_miss", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32) SELECT src.id`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.where
		miss := newSQLSingleSourceExecRow("src", SQLRow{"id": int64(99)})
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, miss); value != false {
				b.Fatalf("numeric IN miss = %#v, want false", value)
			}
		}
	})

	b.Run("string_8_hit", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.key IN ('key-1', 'key-3', 'key-5', 'key-7', 'key-9', 'key-11', 'key-13', 'key-31') SELECT src.key`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.where
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value != true {
				b.Fatalf("string IN = %#v, want true", value)
			}
		}
	})
}
