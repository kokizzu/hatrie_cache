package hatCache

import "testing"

func BenchmarkHatTrieSQLCompositeJSONIndexDiagnostics(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("users", `[{"id":1,"team_id":20,"enabled":true},{"id":2,"team_id":20,"enabled":false},{"id":3,"team_id":20,"enabled":true},{"id":4,"team_id":30,"enabled":true}]`)
	if err := trie.CreateSQLJSONCompositeIndex("users", "team_id", "enabled"); err != nil {
		b.Fatal(err)
	}
	query := "EXPLAIN ANALYZE FROM CACHE('users') AS users WHERE users.team_id = 20 AND users.enabled = TRUE SELECT users.id ORDER BY users.id"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHatTrieSQLCompositeJSONIndexQuery(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("users", `[{"id":1,"team_id":20,"enabled":true},{"id":2,"team_id":20,"enabled":false},{"id":3,"team_id":20,"enabled":true},{"id":4,"team_id":30,"enabled":true}]`)
	if err := trie.CreateSQLJSONCompositeIndex("users", "team_id", "enabled"); err != nil {
		b.Fatal(err)
	}
	query := "FROM CACHE('users') AS users WHERE users.team_id = 20 AND users.enabled = TRUE SELECT users.id ORDER BY users.id"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			b.Fatal(err)
		}
	}
}
