package hatSql_test

import (
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLQueryFingerprintIgnoresLiteralValues(t *testing.T) {
	first, err := hatSql.SQLQueryFingerprint("select * from users where id = 1 and name = 'alice' and score = 1.5")
	if err != nil {
		t.Fatalf("fingerprint first query: %v", err)
	}
	second, err := hatSql.SQLQueryFingerprint(" SELECT * FROM users WHERE id = 99 AND name = 'bob' AND score = 9.25 ")
	if err != nil {
		t.Fatalf("fingerprint second query: %v", err)
	}
	if first != second {
		t.Fatalf("literal values changed fingerprint: %q != %q", first, second)
	}
	if len(first) != 64 {
		t.Fatalf("fingerprint length = %d, want SHA-256 hex length 64", len(first))
	}
	for _, value := range []string{"alice", "bob", "1.5", "9.25"} {
		if strings.Contains(first, value) {
			t.Fatalf("fingerprint contains literal value %q: %q", value, first)
		}
	}
}

func TestSQLQueryFingerprintPreservesLiteralTypesAndShape(t *testing.T) {
	base := "SELECT * FROM users WHERE id = 1"
	cases := []struct {
		name  string
		query string
	}{
		{name: "real literal", query: "SELECT * FROM users WHERE id = 1.0"},
		{name: "different column", query: "SELECT * FROM users WHERE account_id = 1"},
		{name: "different operator", query: "SELECT * FROM users WHERE id > 1"},
		{name: "different parameter position", query: "SELECT * FROM users WHERE id = $2"},
	}

	baseFingerprint, err := hatSql.SQLQueryFingerprint(base)
	if err != nil {
		t.Fatalf("fingerprint base query: %v", err)
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fingerprint, err := hatSql.SQLQueryFingerprint(test.query)
			if err != nil {
				t.Fatalf("fingerprint query: %v", err)
			}
			if fingerprint == baseFingerprint {
				t.Fatalf("query shape was not distinguished: %q", test.query)
			}
		})
	}
}

func TestSQLQueryFingerprintPreservesParameterPositions(t *testing.T) {
	first, err := hatSql.SQLQueryFingerprint("SELECT * FROM users WHERE id = $1")
	if err != nil {
		t.Fatalf("fingerprint first parameter query: %v", err)
	}
	second, err := hatSql.SQLQueryFingerprint("SELECT * FROM users WHERE id = $2")
	if err != nil {
		t.Fatalf("fingerprint second parameter query: %v", err)
	}
	if first == second {
		t.Fatal("different parameter positions share a fingerprint")
	}
	normalized, err := hatSql.SQLQueryFingerprint("SELECT * FROM users WHERE id = $01")
	if err != nil {
		t.Fatalf("fingerprint normalized parameter query: %v", err)
	}
	if first != normalized {
		t.Fatal("equivalent parameter indexes do not share a fingerprint")
	}
}

func TestSQLQueryFingerprintRejectsInvalidSQL(t *testing.T) {
	if _, err := hatSql.SQLQueryFingerprint("SELECT * FROM users WHERE name = 'unterminated"); err == nil {
		t.Fatal("expected invalid SQL to return an error")
	}
}

func BenchmarkSQLQueryFingerprint(b *testing.B) {
	query := "SELECT id, name FROM users WHERE tenant_id = 7 AND score >= 10.5 AND name = 'alice' ORDER BY id LIMIT 100"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := hatSql.SQLQueryFingerprint(query); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFormatSQLForFingerprintBaseline(b *testing.B) {
	query := "SELECT id, name FROM users WHERE tenant_id = 7 AND score >= 10.5 AND name = 'alice' ORDER BY id LIMIT 100"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := hatSql.FormatSQL(query); err != nil {
			b.Fatal(err)
		}
	}
}
