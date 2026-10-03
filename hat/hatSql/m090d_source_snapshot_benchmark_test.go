package hatSql

import (
	"fmt"
	"testing"
)

type m090dBenchmarkResolver struct {
	rows  []Row
	calls int
	keys  []string
}

func (resolver *m090dBenchmarkResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	resolver.calls++
	resolver.keys = append(resolver.keys, name+"/"+key)
	return CloneRows(resolver.rows), nil
}

func TestM090dNativeDataflowSharesSourceSnapshot(t *testing.T) {
	resolver := &m090dBenchmarkResolver{rows: []Row{{"id": int64(1), "version": "first"}}}
	result, err := ExecuteSQLQuery(`FROM CACHE('users') AS left_users
INNER JOIN CACHE('users') AS right_users ON left_users.id = right_users.id
SELECT left_users.version AS left_version, right_users.version AS right_version`, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want one snapshot read", resolver.calls)
	}
	want := []Row{{"left_version": "first", "right_version": "first"}}
	if len(result.Rows) != 1 || result.Rows[0]["left_version"] != want[0]["left_version"] || result.Rows[0]["right_version"] != want[0]["right_version"] {
		t.Fatalf("snapshot result = %#v, want %#v", result.Rows, want)
	}
}

var m090dBenchmarkResult SQLQueryResult

func BenchmarkM090dRepeatedSourceJoin(b *testing.B) {
	rows := make([]Row, 1024)
	for index := range rows {
		rows[index] = Row{
			"id":      int64(index),
			"version": fmt.Sprintf("version-%04d", index),
		}
	}
	resolver := &m090dBenchmarkResolver{rows: rows}
	query := `FROM CACHE('users') AS left_users
INNER JOIN CACHE('users') AS right_users ON left_users.id = right_users.id
SELECT left_users.id, left_users.version, right_users.version`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		resolver.calls = 0
		resolver.keys = resolver.keys[:0]
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		if resolver.calls != 1 {
			b.Fatalf("resolver calls = %d, want one snapshot read", resolver.calls)
		}
		m090dBenchmarkResult = result
	}
}
