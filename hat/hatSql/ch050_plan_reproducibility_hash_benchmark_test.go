package hatSql

import "testing"

func BenchmarkCH050PlanReproducibilityHash(b *testing.B) {
	estimatedRows := 128
	input := SQLPlanReproducibilityInput{
		Query:               "SELECT id, name FROM users WHERE tenant_id = 7 AND score >= 10.5 ORDER BY id LIMIT 100",
		SchemaFingerprint:   "schema-users-v17",
		SettingsFingerprint: "settings-default-v3",
		Steps: []ExplainStep{
			{Node: "SCAN", Detail: "users", EstimatedRows: &estimatedRows},
			{Node: "FILTER", Detail: "tenant_id = ? AND score >= ?"},
			{Node: "SORT", Detail: "id ASC"},
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := SQLPlanReproducibilityHash(input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH050QueryFingerprintBaseline(b *testing.B) {
	query := "SELECT id, name FROM users WHERE tenant_id = 7 AND score >= 10.5 ORDER BY id LIMIT 100"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := SQLQueryFingerprint(query); err != nil {
			b.Fatal(err)
		}
	}
}
