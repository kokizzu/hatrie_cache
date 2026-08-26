package hatCache

import (
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

type sqlCompatibilityCase struct {
	name      string
	hatrie    string
	sqlite    string
	postgres  string
	mariadb   string
	mariaJSON string
}

var sqlCompatibilityCases = []sqlCompatibilityCase{
	{
		name:      "filter-order",
		hatrie:    "FROM VALUES (1, 'alpha'), (2, 'beta'), (3, 'beta') AS source(id, kind) WHERE source.id >= 2 SELECT source.id, source.kind ORDER BY source.id",
		sqlite:    "WITH source(id, kind) AS (VALUES (1, 'alpha'), (2, 'beta'), (3, 'beta')) SELECT id, kind FROM source WHERE id >= 2 ORDER BY id",
		postgres:  "WITH source(id, kind) AS (VALUES (1, 'alpha'), (2, 'beta'), (3, 'beta')) SELECT id, kind FROM source WHERE id >= 2 ORDER BY id",
		mariadb:   "SELECT id, kind FROM (SELECT 1 AS id, 'alpha' AS kind UNION ALL SELECT 2, 'beta' UNION ALL SELECT 3, 'beta') AS source_rows WHERE id >= 2 ORDER BY id",
		mariaJSON: "JSON_OBJECT('id', id, 'kind', kind)",
	},
	{
		name:      "aggregate-group",
		hatrie:    "FROM VALUES ('alpha', 2), ('alpha', 3), ('beta', 5) AS source(kind, score) SELECT source.kind, SUM(source.score) AS total GROUP BY source.kind ORDER BY source.kind",
		sqlite:    "WITH source(kind, score) AS (VALUES ('alpha', 2), ('alpha', 3), ('beta', 5)) SELECT kind, SUM(score) AS total FROM source GROUP BY kind ORDER BY kind",
		postgres:  "WITH source(kind, score) AS (VALUES ('alpha', 2), ('alpha', 3), ('beta', 5)) SELECT kind, SUM(score) AS total FROM source GROUP BY kind ORDER BY kind",
		mariadb:   "SELECT kind, SUM(score) AS total FROM (SELECT 'alpha' AS kind, 2 AS score UNION ALL SELECT 'alpha', 3 UNION ALL SELECT 'beta', 5) AS source_rows GROUP BY kind ORDER BY kind",
		mariaJSON: "JSON_OBJECT('kind', kind, 'total', total)",
	},
}

func TestSQLCompatibilitySQLite(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 is required for SQL compatibility suite")
	}
	for _, test := range sqlCompatibilityCases {
		t.Run(test.name, func(t *testing.T) {
			got := runHatrieCompatibilityCase(t, test.hatrie)
			output, err := exec.Command("sqlite3", "-json", ":memory:", test.sqlite).Output()
			if err != nil {
				t.Fatalf("sqlite query error = %v", err)
			}
			assertCompatibilityRows(t, got, output)
		})
	}
}

func TestSQLCompatibilityPostgres(t *testing.T) {
	url := strings.TrimSpace(os.Getenv("HATRIE_POSTGRES_URL"))
	if url == "" {
		t.Skip("HATRIE_POSTGRES_URL is required for PostgreSQL compatibility suite")
	}
	if _, err := exec.LookPath("psql"); err != nil {
		t.Skip("psql is required for PostgreSQL compatibility suite")
	}
	for _, test := range sqlCompatibilityCases {
		t.Run(test.name, func(t *testing.T) {
			got := runHatrieCompatibilityCase(t, test.hatrie)
			query := "SELECT COALESCE(json_agg(row_to_json(result)), '[]'::json)::text FROM (" + test.postgres + ") AS result"
			output, err := exec.Command("psql", url, "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", query).Output()
			if err != nil {
				t.Fatalf("PostgreSQL compatibility query error = %v", err)
			}
			assertCompatibilityRows(t, got, output)
		})
	}
}

func TestSQLCompatibilityMariaDB(t *testing.T) {
	database := strings.TrimSpace(os.Getenv("HATRIE_MARIADB_DATABASE"))
	if database == "" {
		t.Skip("HATRIE_MARIADB_DATABASE is required for MariaDB compatibility suite")
	}
	if _, err := exec.LookPath("mariadb"); err != nil {
		t.Skip("mariadb client is required for MariaDB compatibility suite")
	}
	defaultsFile := strings.TrimSpace(os.Getenv("HATRIE_MARIADB_DEFAULTS_FILE"))
	for _, test := range sqlCompatibilityCases {
		t.Run(test.name, func(t *testing.T) {
			got := runHatrieCompatibilityCase(t, test.hatrie)
			query := "SELECT COALESCE(JSON_ARRAYAGG(" + test.mariaJSON + "), JSON_ARRAY()) FROM (" + test.mariadb + ") AS result"
			output, err := exec.Command("mariadb", mariadbCompatibilityArgs(database, defaultsFile, query)...).Output()
			if err != nil {
				t.Fatalf("MariaDB compatibility query error = %v", err)
			}
			assertCompatibilityRows(t, got, output)
		})
	}
}

func TestMariaDBCompatibilityArgsUseDefaultsFileBeforeOtherOptions(t *testing.T) {
	got := mariadbCompatibilityArgs("hatrie_compat", "/run/secrets/client.cnf", "SELECT 1")
	want := []string{"--defaults-extra-file=/run/secrets/client.cnf", "--batch", "--skip-column-names", "--raw", "--database=hatrie_compat", "--execute", "SELECT 1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mariadbCompatibilityArgs() = %#v, want %#v", got, want)
	}
}

func mariadbCompatibilityArgs(database string, defaultsFile string, query string) []string {
	args := make([]string, 0, 6)
	if defaultsFile != "" {
		args = append(args, "--defaults-extra-file="+defaultsFile)
	}
	args = append(args, "--batch", "--skip-column-names", "--raw", "--database="+database, "--execute", query)
	return args
}

func runHatrieCompatibilityCase(t *testing.T, query string) []byte {
	t.Helper()
	result, err := ExecuteSQLQuery(query, SQLSourceResolverFunc(nil))
	if err != nil {
		t.Fatalf("hatrie query error = %v", err)
	}
	encoded, err := json.Marshal(result.Rows)
	if err != nil {
		t.Fatalf("marshal hatrie rows error = %v", err)
	}
	return encoded
}

func assertCompatibilityRows(t *testing.T, got, wantJSON []byte) {
	t.Helper()
	var actual, expected []map[string]interface{}
	if err := json.Unmarshal(got, &actual); err != nil {
		t.Fatalf("decode hatrie rows error = %v", err)
	}
	if err := json.Unmarshal(wantJSON, &expected); err != nil {
		t.Fatalf("decode reference rows error = %v; output=%q", err, wantJSON)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("SQL compatibility mismatch\nhatrie=%#v\nreference=%#v", actual, expected)
	}
}
