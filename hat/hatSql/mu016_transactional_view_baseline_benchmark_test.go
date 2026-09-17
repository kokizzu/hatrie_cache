package hatSql

import "testing"

func BenchmarkMU016ViewCreateBaseline(b *testing.B) {
	query := `FROM VALUES ('Ada') AS rows(name) SELECT name`
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		session := NewSQLSession(nil)
		if err := session.CreateView("names", query); err != nil {
			b.Fatal(err)
		}
	}
}
