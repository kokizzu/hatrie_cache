package hatSql

import (
	"testing"
	"time"
)

func TestCH052TemporalLiteralSemantics(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT PARSE_TIMESTAMP(src.text, 'Asia/Singapore') AS parsed, src.instant AT TIME ZONE 'Asia/Singapore' AS converted, TIMESTAMP_ADD(src.instant, DURATION '90m') AS later`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.selects[0].expr.timeZoneProgram == nil || query.selects[1].expr.timeZoneProgram == nil {
		t.Fatal("literal time zone was not prepared during query binding")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{
		"text":    "2026-08-22 09:30:00",
		"instant": time.Date(2026, time.August, 22, 1, 30, 0, 0, time.UTC),
	})
	group := []sqlExecRow{row}
	parsed := evalSQLExpr(query.selects[0].expr, group, row)
	if value, ok := parsed.(time.Time); !ok || value.Location().String() != "Asia/Singapore" || value.Hour() != 9 || value.Minute() != 30 {
		t.Fatalf("PARSE_TIMESTAMP() = %#v, want Singapore 09:30", parsed)
	}
	converted := evalSQLExpr(query.selects[1].expr, group, row)
	if value, ok := converted.(time.Time); !ok || value.Location().String() != "Asia/Singapore" || value.Hour() != 9 || value.Minute() != 30 {
		t.Fatalf("AT TIME ZONE = %#v, want Singapore 09:30", converted)
	}
	later := evalSQLExpr(query.selects[2].expr, group, row)
	if value, ok := later.(time.Time); !ok || !value.Equal(time.Date(2026, time.August, 22, 3, 0, 0, 0, time.UTC)) {
		t.Fatalf("TIMESTAMP_ADD() = %#v, want 03:00 UTC", later)
	}
}

func TestCH052TemporalDynamicZoneFallback(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT PARSE_TIMESTAMP(src.text, src.zone) AS parsed`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.selects[0].expr.timeZoneProgram != nil {
		t.Fatal("dynamic time zone unexpectedly received a literal program")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{
		"text": "2026-08-22 09:30:00",
		"zone": "Asia/Singapore",
	})
	value := evalSQLExpr(query.selects[0].expr, []sqlExecRow{row}, row)
	if parsed, ok := value.(time.Time); !ok || parsed.Location().String() != "Asia/Singapore" || parsed.Hour() != 9 || parsed.Minute() != 30 {
		t.Fatalf("dynamic PARSE_TIMESTAMP() = %#v, want Singapore 09:30", value)
	}
}

func TestCH052TemporalParameterZonePreparation(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src SELECT PARSE_TIMESTAMP(src.text, $1) AS parsed`, []interface{}{"Asia/Singapore"})
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	if query.selects[0].expr.timeZoneProgram == nil {
		t.Fatal("bound literal time zone was not prepared")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"text": "2026-08-22 09:30:00"})
	value := evalSQLExpr(query.selects[0].expr, []sqlExecRow{row}, row)
	if parsed, ok := value.(time.Time); !ok || parsed.Location().String() != "Asia/Singapore" || parsed.Hour() != 9 || parsed.Minute() != 30 {
		t.Fatalf("bound PARSE_TIMESTAMP() = %#v, want Singapore 09:30", value)
	}
}

func TestCH052TemporalInvalidZoneKeepsEvaluationError(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT PARSE_TIMESTAMP(src.text, 'Mars/Olympus') AS parsed`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.selects[0].expr.timeZoneProgram == nil || query.selects[0].expr.timeZoneProgram.err == nil {
		t.Fatal("invalid literal time zone was not retained as a failed program")
	}
	row := newSQLSingleSourceExecRow("src", SQLRow{"text": "2026-08-22 09:30:00"})
	value := evalSQLExpr(query.selects[0].expr, []sqlExecRow{row}, row)
	if err := sqlExpressionError(value); err == nil {
		t.Fatalf("invalid PARSE_TIMESTAMP() = %#v, want evaluation error", value)
	}
}

func BenchmarkCH052TemporalLiteralEvaluation(b *testing.B) {
	row := newSQLSingleSourceExecRow("src", SQLRow{
		"text":    "2026-08-22 09:30:00",
		"instant": time.Date(2026, time.August, 22, 1, 30, 0, 0, time.UTC),
		"zone":    "Asia/Singapore",
	})

	b.Run("parse_literal_zone", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT PARSE_TIMESTAMP(src.text, 'Asia/Singapore') AS parsed`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.selects[0].expr
		group := []sqlExecRow{row}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value == nil {
				b.Fatalf("PARSE_TIMESTAMP() returned nil")
			}
		}
	})

	b.Run("parse_all_literals", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT PARSE_TIMESTAMP('2026-08-22 09:30:00', 'Asia/Singapore') AS parsed`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.selects[0].expr
		group := []sqlExecRow{row}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value == nil {
				b.Fatalf("PARSE_TIMESTAMP() returned nil")
			}
		}
	})

	b.Run("parse_dynamic_zone", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT PARSE_TIMESTAMP(src.text, src.zone) AS parsed`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.selects[0].expr
		group := []sqlExecRow{row}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value == nil {
				b.Fatalf("PARSE_TIMESTAMP() returned nil")
			}
		}
	})

	b.Run("timezone_literal", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT src.instant AT TIME ZONE 'Asia/Singapore' AS converted`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.selects[0].expr
		group := []sqlExecRow{row}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value == nil {
				b.Fatalf("AT TIME ZONE returned nil")
			}
		}
	})

	b.Run("timezone_all_literals", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT TIMESTAMP '2026-08-22T01:30:00Z' AT TIME ZONE 'Asia/Singapore' AS converted`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.selects[0].expr
		group := []sqlExecRow{row}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value == nil {
				b.Fatalf("AT TIME ZONE returned nil")
			}
		}
	})

	b.Run("timestamp_add", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT TIMESTAMP_ADD(src.instant, DURATION '90m') AS later`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.selects[0].expr
		group := []sqlExecRow{row}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value == nil {
				b.Fatalf("TIMESTAMP_ADD() returned nil")
			}
		}
	})

	b.Run("timestamp_add_all_literals", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT TIMESTAMP_ADD(TIMESTAMP '2026-08-22T01:30:00Z', DURATION '90m') AS later`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.selects[0].expr
		group := []sqlExecRow{row}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value == nil {
				b.Fatalf("TIMESTAMP_ADD() returned nil")
			}
		}
	})

	b.Run("timestamp_diff_all_literals", func(b *testing.B) {
		query, err := parseSQLQuery(`FROM CACHE('events') AS src SELECT TIMESTAMP_DIFF(TIMESTAMP '2026-08-22T03:00:00Z', TIMESTAMP '2026-08-22T01:30:00Z') AS elapsed`)
		if err != nil {
			b.Fatalf("parseSQLQuery() error = %v", err)
		}
		expr := query.selects[0].expr
		group := []sqlExecRow{row}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if value := evalSQLExpr(expr, group, row); value == nil {
				b.Fatalf("TIMESTAMP_DIFF() returned nil")
			}
		}
	})
}
