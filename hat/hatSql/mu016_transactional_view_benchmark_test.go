package hatSql

import "testing"

func BenchmarkMU016TransactionalViewBatch(b *testing.B) {
	changes := []SQLSessionViewChange{
		{Name: "derived", Query: `FROM CACHE('base') SELECT name`},
		{Name: "base", Query: `FROM VALUES ('Ada') AS rows(name) SELECT name`},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		session := NewSQLSession(nil)
		if _, err := session.ApplyViewChanges(changes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU016TransactionalViewReplace(b *testing.B) {
	initial := []SQLSessionViewChange{
		{Name: "derived", Query: `FROM CACHE('base') SELECT name`},
		{Name: "base", Query: `FROM VALUES ('Ada') AS rows(name) SELECT name`},
	}
	replacement := []SQLSessionViewChange{
		{Name: "base", Query: `FROM VALUES ('Lin') AS rows(name) SELECT name`},
		{Name: "derived", Query: `FROM CACHE('base') SELECT name`},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		session := NewSQLSession(nil)
		if _, err := session.ApplyViewChanges(initial); err != nil {
			b.Fatal(err)
		}
		if _, err := session.ApplyViewChanges(replacement); err != nil {
			b.Fatal(err)
		}
	}
}
