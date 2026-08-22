package hatriecache

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestSQLMarkdownBeginnerWalkthroughDocumentsEveryPublicCommand(t *testing.T) {
	data, err := os.ReadFile("SQL.md")
	if err != nil {
		t.Fatalf("ReadFile(SQL.md) error = %v", err)
	}
	doc := string(data)
	for _, token := range []string{
		"## First SQL session", "Before cache state", "After cache state",
		"## Relational query walkthrough", "Command SQL", "Relational SQL",
		"TestSQLGuideCommandExamples", "TestSQLGuideRelationalExamples",
		"DATA_STRUCTURE.md",
	} {
		if !strings.Contains(doc, token) {
			t.Fatalf("SQL.md missing beginner-guide token %q", token)
		}
	}
	for _, command := range publicSQLCommandNames() {
		if !strings.Contains(doc, "`"+command+"`") {
			t.Fatalf("SQL.md does not document public SQL command %q", command)
		}
	}
}

// These flows are the executable counterpart to the copyable examples in
// SQL.md. They prove the shown statement results and DML state transitions.
func TestSQLGuideCommandExamples(t *testing.T) {
	ht := CreateHatTrie()
	t.Cleanup(ht.Destroy)

	run := func(source string) CacheCommandResponse {
		t.Helper()
		request, err := CompileSQL(source)
		if err != nil {
			t.Fatalf("CompileSQL(%q) error = %v", source, err)
		}
		response := ht.ExecuteCommand(request)
		if !response.OK {
			t.Fatalf("ExecuteCommand(%q) response = %#v", source, response)
		}
		return response
	}

	if got := run("SELECT exists FROM cache WHERE key = 'name'").Value; got != "0" {
		t.Fatalf("initial EXISTS = %q, want 0", got)
	}
	run("INSERT INTO cache (key, value) VALUES ('name', 'Ivi')")
	if got := run("SELECT value FROM cache WHERE key = 'name'").Value; got != "Ivi" {
		t.Fatalf("SELECT value after INSERT = %q, want Ivi", got)
	}
	run("UPDATE cache SET value = 'Ada' WHERE key = 'name'")
	if got := run("CALL GET('name')").Value; got != "Ada" {
		t.Fatalf("GET after UPDATE = %q, want Ada", got)
	}

	run("INSERT INTO cache (key, counter) VALUES ('views', 41)")
	if got := run("UPDATE cache SET value = value + 1 WHERE key = 'views'").Value; got != "42" {
		t.Fatalf("INC result = %q, want 42", got)
	}
	run("INSERT INTO cache (key, value, ttl_seconds) VALUES ('temporary', 'yes', 60)")
	if got := run("SELECT ttl FROM cache WHERE key = 'temporary'").Value; got == "-1" || got == "0" {
		t.Fatalf("TTL after INSERT = %q, want positive ttl", got)
	}
	run("CALL PERSIST(key => 'temporary')")
	if got := run("SELECT ttl FROM cache WHERE key = 'temporary'").Value; got != "-1" {
		t.Fatalf("TTL after PERSIST = %q, want -1", got)
	}

	batch := run("INSERT INTO cache (key, value) VALUES ('batch:name', 'first'); UPDATE cache SET value = 'second' WHERE key = 'batch:name'; SELECT value FROM cache WHERE key = 'batch:name'")
	if len(batch.Responses) != 3 || batch.Responses[2].Value != "second" {
		t.Fatalf("multi-statement batch = %#v, want final value second", batch)
	}

	run("CALL MAP.PUT(key => 'user:1', pairs => JSON '{\"name\":\"Ivi\"}')")
	if got := run("CALL MAP.PEEK(key => 'user:1', subkey => 'name')").Value; got != "Ivi" {
		t.Fatalf("MAP.PEEK result = %q, want Ivi", got)
	}
	run("CALL SET.ADD(key => 'tags', values => JSON '[\"go\",\"cache\"]')")
	if got := run("CALL SET.HAS(key => 'tags', value => 'go')").Value; got != "1" {
		t.Fatalf("SET.HAS result = %q, want 1", got)
	}

	run("DELETE FROM cache WHERE key = 'name'")
	if got := run("SELECT exists FROM cache WHERE key = 'name'").Value; got != "0" {
		t.Fatalf("EXISTS after DELETE = %q, want 0", got)
	}
}

func TestSQLGuideRelationalExamples(t *testing.T) {
	ht := CreateHatTrie()
	t.Cleanup(ht.Destroy)
	ht.UpsertBytes("users", []byte(`[
  {"id":1,"name":"Ada","team_id":10,"enabled":true,"score":9},
  {"id":2,"name":"Ivi","team_id":10,"enabled":true,"score":7},
  {"id":3,"name":"Noa","team_id":20,"enabled":false,"score":7}
]`))
	ht.UpsertBytes("teams", []byte(`[
  {"id":10,"name":"Core"},
  {"id":20,"name":"Edge"}
]`))

	run := func(source string) SQLQueryResult {
		t.Helper()
		result, err := ExecuteSQLQuery(source, ht)
		if err != nil {
			t.Fatalf("ExecuteSQLQuery(%q) error = %v", source, err)
		}
		return result
	}

	filtered := run(`
FROM CACHE('users') AS u
WHERE u.enabled = TRUE AND u.score >= 7
SELECT u.name, u.score
ORDER BY u.score DESC, u.name ASC`)
	if want := []SQLRow{{"name": "Ada", "score": float64(9)}, {"name": "Ivi", "score": float64(7)}}; !reflect.DeepEqual(filtered.Rows, want) {
		t.Fatalf("filtered query rows = %#v, want %#v", filtered.Rows, want)
	}

	joined := run(`
FROM CACHE('users') AS u
LEFT JOIN CACHE('teams') AS t ON u.team_id = t.id
WHERE u.enabled = TRUE
SELECT u.name, t.name AS team
ORDER BY u.name`)
	if want := []SQLRow{{"name": "Ada", "team": "Core"}, {"name": "Ivi", "team": "Core"}}; !reflect.DeepEqual(joined.Rows, want) {
		t.Fatalf("join query rows = %#v, want %#v", joined.Rows, want)
	}

	aggregated := run(`
FROM CACHE('users') AS u
SELECT u.team_id, COUNT(*) AS members, SUM(u.score) AS total_score
GROUP BY u.team_id
HAVING COUNT(*) >= 1
ORDER BY u.team_id`)
	if want := []SQLRow{
		{"team_id": float64(10), "members": int64(2), "total_score": float64(16)},
		{"team_id": float64(20), "members": int64(1), "total_score": float64(7)},
	}; !reflect.DeepEqual(aggregated.Rows, want) {
		t.Fatalf("aggregate query rows = %#v, want %#v", aggregated.Rows, want)
	}

	values := run(`
WITH scores(name, score) AS (VALUES ('Ada', 9), ('Ivi', 7), ('Noa', 7))
SELECT DISTINCT name, score, RANK() OVER (ORDER BY score DESC) AS place
FROM scores
ORDER BY score DESC, name ASC
LIMIT 2`)
	if want := []SQLRow{
		{"name": "Ada", "score": int64(9), "place": int64(1)},
		{"name": "Ivi", "score": int64(7), "place": int64(2)},
	}; !reflect.DeepEqual(values.Rows, want) {
		t.Fatalf("values/window query rows = %#v, want %#v", values.Rows, want)
	}

	set := run(`SELECT value FROM VALUES (1), (2) AS left_rows(value)
UNION
SELECT value FROM VALUES (2), (3) AS right_rows(value)
ORDER BY value`)
	if want := []SQLRow{{"value": int64(1)}, {"value": int64(2)}, {"value": int64(3)}}; !reflect.DeepEqual(set.Rows, want) {
		t.Fatalf("UNION query rows = %#v, want %#v", set.Rows, want)
	}

	explain := run(`EXPLAIN FROM VALUES (1), (2) AS rows(value) SELECT value ORDER BY value`)
	if len(explain.Plan) == 0 || len(explain.Rows) == 0 {
		t.Fatalf("EXPLAIN result = %#v, want plan rows", explain)
	}
}
