package hatriecache

import (
	"reflect"
	"strings"
	"testing"
)

func TestExecuteSQLQueryWithLeftJoinGroupingHavingAndOrder(t *testing.T) {
	t.Parallel()

	source := `
WITH users (id, team_id, name) AS (
  VALUES (1, 10, 'Ivi'), (2, 10, 'Lia'), (3, 20, 'No team')
), teams (id, name) AS (
  VALUES (10, 'Core')
)
SELECT t.name AS team, COUNT(*) AS members
FROM users AS u
LEFT JOIN teams AS t ON u.team_id = t.id
WHERE u.name IS NOT NULL
GROUP BY t.name
HAVING COUNT(*) > 0
ORDER BY members DESC, team ASC`

	result, err := ExecuteSQLQuery(source, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{
		Columns: []string{"team", "members"},
		Rows: []SQLRow{
			{"team": "Core", "members": int64(2)},
			{"team": nil, "members": int64(1)},
		},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("ExecuteSQLQuery() = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQuerySupportsCacheSourcesCrossJoinAndPredicates(t *testing.T) {
	t.Parallel()

	resolver := SQLSourceResolverFunc(func(name string, key string) ([]SQLRow, error) {
		if name != "CACHE" {
			return nil, nil
		}
		switch key {
		case "users":
			return []SQLRow{{"id": int64(1), "name": "Ivi"}, {"id": int64(2), "name": "Lia"}}, nil
		case "regions":
			return []SQLRow{{"region": "apac"}, {"region": "eu"}}, nil
		default:
			return nil, nil
		}
	})

	result, err := ExecuteSQLQuery(`
SELECT u.name, r.region
FROM CACHE('users') AS u
CROSS JOIN CACHE('regions') AS r
WHERE u.name LIKE 'I%'
ORDER BY r.region DESC`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{
		Columns: []string{"name", "region"},
		Rows: []SQLRow{
			{"name": "Ivi", "region": "eu"},
			{"name": "Ivi", "region": "apac"},
		},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("ExecuteSQLQuery() = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQueryAcceptsSourceFirstClauseOrder(t *testing.T) {
	t.Parallel()

	result, err := ExecuteSQLQuery(`
FROM VALUES (1, 'Ivi'), (2, 'Lia') AS users(id, name)
WHERE id > 1
SELECT name
ORDER BY name`, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"name"}, Rows: []SQLRow{{"name": "Lia"}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("ExecuteSQLQuery() = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQueryReportsJoinSuggestion(t *testing.T) {
	t.Parallel()

	source := "SELECT * FROM VALUES (1) AS a JION VALUES (1) AS b ON a.column1 = b.column1"
	_, err := ExecuteSQLQuery(source, SQLSourceResolverFunc(nil))
	if err == nil {
		t.Fatal("ExecuteSQLQuery() error = nil, want syntax error")
	}
	formatted := FormatSQLDiagnostic(source, err)
	for _, want := range []string{"expected JOIN", "did you mean `JOIN`?", "JION"} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("FormatSQLDiagnostic() = %q, want substring %q", formatted, want)
		}
	}
}
