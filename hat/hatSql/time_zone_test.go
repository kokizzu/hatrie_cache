package hatSql_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLTimeZoneParsingAndArithmetic(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`FROM VALUES (1) AS src(n) SELECT PARSE_TIMESTAMP('2026-08-22 09:30:00', 'Asia/Singapore') AS local_time, TIMESTAMP_ADD(PARSE_TIMESTAMP('2026-08-22 09:30:00', 'Asia/Singapore'), DURATION '90m') AS later, TIMESTAMP_DIFF(TIMESTAMP_ADD(PARSE_TIMESTAMP('2026-08-22 09:30:00', 'Asia/Singapore'), DURATION '90m'), PARSE_TIMESTAMP('2026-08-22 09:30:00', 'Asia/Singapore')) AS elapsed, TIMESTAMP '2026-08-22T01:30:00Z' AT TIME ZONE 'Asia/Singapore' AS converted`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %#v, want one row", result.Rows)
	}
	row := result.Rows[0]
	local, ok := row["local_time"].(time.Time)
	if !ok || local.Location().String() != "Asia/Singapore" || local.Hour() != 9 || local.Minute() != 30 {
		t.Fatalf("local_time = %#v, want Singapore 09:30", row["local_time"])
	}
	later, ok := row["later"].(time.Time)
	if !ok || later.Hour() != 11 || later.Minute() != 0 || later.Location().String() != "Asia/Singapore" {
		t.Fatalf("later = %#v, want Singapore 11:00", row["later"])
	}
	elapsed, ok := row["elapsed"].(hatSql.SQLDuration)
	if !ok || string(elapsed) != "1h30m0s" {
		t.Fatalf("elapsed = %#v, want SQLDuration 1h30m0s", row["elapsed"])
	}
	converted, ok := row["converted"].(time.Time)
	if !ok || converted.Location().String() != "Asia/Singapore" || converted.Hour() != 9 || converted.Minute() != 30 {
		t.Fatalf("converted = %#v, want Singapore 09:30", row["converted"])
	}
}

func TestSQLTimeZoneDiagnostics(t *testing.T) {
	_, err := hatSql.ExecuteSQLQuery(`FROM VALUES (1) AS src(n) SELECT PARSE_TIMESTAMP('2026-08-22 09:30:00', 'Mars/Olympus')`, nil)
	if err == nil || !containsSQLTimeZoneError(err.Error(), "unknown time zone") {
		t.Fatalf("invalid zone error = %v", err)
	}
	_, err = hatSql.ExecuteSQLQuery(`FROM VALUES (1) AS src(n) SELECT TIMESTAMP_ADD(TIMESTAMP '2026-08-22T01:30:00Z', DURATION 'bad')`, nil)
	if err == nil || !containsSQLTimeZoneError(err.Error(), "DURATION requires") {
		t.Fatalf("invalid duration error = %v", err)
	}
}

func containsSQLTimeZoneError(value, expected string) bool {
	return len(value) >= len(expected) && (value == expected || containsTimeZoneSubstring(value, expected))
}

func containsTimeZoneSubstring(value, expected string) bool {
	for index := 0; index+len(expected) <= len(value); index++ {
		if value[index:index+len(expected)] == expected {
			return true
		}
	}
	return false
}
