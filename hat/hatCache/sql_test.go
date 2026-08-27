package hatCache

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestCompileSQLScalarStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		want   CacheCommandRequest
	}{
		{
			name:   "select value",
			source: "SELECT value FROM cache WHERE key = 'user:1'",
			want:   CacheCommandRequest{Command: "GETSTR", Key: "user:1"},
		},
		{
			name:   "insert string with ttl",
			source: "INSERT INTO cache (key, value, ttl_seconds) VALUES ('session:1', 'active', 60)",
			want:   CacheCommandRequest{Command: "SETSTRX", Key: "session:1", Value: "active", TTLSeconds: int64Pointer(60)},
		},
		{
			name:   "increment counter",
			source: "UPDATE cache SET value = value + 2 WHERE key = 'views'",
			want:   CacheCommandRequest{Command: "INC", Key: "views", Value: "2"},
		},
		{
			name:   "delete",
			source: "DELETE FROM cache WHERE key = 'session:1'",
			want:   CacheCommandRequest{Command: "DEL", Key: "session:1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := CompileSQL(test.source)
			if err != nil {
				t.Fatalf("CompileSQL() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("CompileSQL() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestCompileSQLCallLosslesslyMapsPublicCommandFields(t *testing.T) {
	t.Parallel()

	got, err := CompileSQL(`CALL PUTMAP(key => 'user:1', pairs => JSON '{"name":"ivi","age":32}')`)
	if err != nil {
		t.Fatalf("CompileSQL() error = %v", err)
	}
	want := CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "user:1",
		Pairs:   Map{"name": "ivi", "age": float64(32)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CompileSQL() = %#v, want %#v", got, want)
	}

	got, err = CompileSQL(`CALL ADDSET(key => 'tags', values => JSON '["go","cache"]')`)
	if err != nil {
		t.Fatalf("CompileSQL() error = %v", err)
	}
	want = CacheCommandRequest{Command: "ADDSET", Key: "tags", Values: Slice{"go", "cache"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CompileSQL() = %#v, want %#v", got, want)
	}
}

func TestCompileSQLAcceptsEveryDocumentedPublicCallName(t *testing.T) {
	t.Parallel()

	for _, command := range publicSQLCommandNames() {
		command := command
		t.Run(command, func(t *testing.T) {
			t.Parallel()
			request, err := CompileSQL("CALL " + command + "(key => 'test-key')")
			if err != nil {
				t.Fatalf("CompileSQL(%s) error = %v", command, err)
			}
			if request.Command != command || request.Key != "test-key" {
				t.Fatalf("CompileSQL(%s) = %#v, want command/key", command, request)
			}
		})
	}
}

func TestCompileSQLCompilesProgramsToOrderedBatch(t *testing.T) {
	t.Parallel()

	got, err := CompileSQL("INSERT INTO cache (key, value) VALUES ('name', 'ivi'); SELECT value FROM cache WHERE key = 'name';")
	if err != nil {
		t.Fatalf("CompileSQL() error = %v", err)
	}
	want := CacheCommandRequest{
		Command: "BATCH",
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: "name", Value: "ivi"},
			{Command: "GETSTR", Key: "name"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CompileSQL() = %#v, want %#v", got, want)
	}
}

func TestCompileSQLCompilesAtomicProgram(t *testing.T) {
	t.Parallel()
	got, err := CompileSQL("BEGIN ATOMIC; INSERT INTO cache (key, value) VALUES ('name', 'ivi'); COMMIT;")
	if err != nil {
		t.Fatalf("CompileSQL() error = %v", err)
	}
	want := CacheCommandRequest{Command: "BATCH", Atomic: true, Batch: []CacheCommandRequest{{Command: "SETSTR", Key: "name", Value: "ivi"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CompileSQL() = %#v, want %#v", got, want)
	}
}

func TestCompileSQLAtomicProgramSupportsSavepoints(t *testing.T) {
	t.Parallel()
	got, err := CompileSQL(`
BEGIN ATOMIC;
INSERT INTO cache (key, value) VALUES ('keep', 'first');
SAVEPOINT before_discard;
INSERT INTO cache (key, value) VALUES ('discard', 'second');
ROLLBACK TO before_discard;
RELEASE SAVEPOINT before_discard;
COMMIT;`)
	if err != nil {
		t.Fatalf("CompileSQL() error = %v", err)
	}
	want := CacheCommandRequest{Command: "BATCH", Atomic: true, Batch: []CacheCommandRequest{{Command: "SETSTR", Key: "keep", Value: "first"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CompileSQL() = %#v, want %#v", got, want)
	}
}

func TestExecuteSQLMutationSupportsInsertSelectAndExistingDML(t *testing.T) {
	trie := newTestTrie(t)
	result, err := ExecuteSQLMutation(context.Background(), trie, `
INSERT INTO cache (key, value)
FROM VALUES ('user:1', 'Ada'), ('user:2', 'Lin') AS rows(key, value)
SELECT key, value`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLMutation(INSERT SELECT) error = %v", err)
	}
	if result.Affected != 2 || !result.Response.OK {
		t.Fatalf("ExecuteSQLMutation(INSERT SELECT) = %#v", result)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "user:2"}); !got.OK || got.Value != "Lin" {
		t.Fatalf("user:2 = %#v, want Lin", got)
	}

	result, err = ExecuteSQLMutation(context.Background(), trie, "UPDATE cache SET value = 'Grace' WHERE key = 'user:1'", nil, SQLQueryOptions{})
	if err != nil || result.Affected != 1 {
		t.Fatalf("ExecuteSQLMutation(UPDATE) = %#v, %v", result, err)
	}
	result, err = ExecuteSQLMutation(context.Background(), trie, "DELETE FROM cache WHERE key = 'user:2'", nil, SQLQueryOptions{})
	if err != nil || result.Affected != 1 {
		t.Fatalf("ExecuteSQLMutation(DELETE) = %#v, %v", result, err)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "user:2"}); !got.OK || got.Value != "0" {
		t.Fatalf("user:2 exists = %#v, want deleted", got)
	}
}

func TestExecuteSQLMutationMergeConditionalAndReturning(t *testing.T) {
	trie := newTestTrie(t)

	result, err := ExecuteSQLMutation(context.Background(), trie, `
MERGE INTO cache (key, value) VALUES ('profile:1', 'Ada')
WHEN NOT MATCHED THEN INSERT
RETURNING key, value, exists`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLMutation(MERGE insert) error = %v", err)
	}
	if result.Affected != 1 || !reflect.DeepEqual(result.Columns, []string{"key", "value", "exists"}) {
		t.Fatalf("ExecuteSQLMutation(MERGE insert) = %#v", result)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"key": "profile:1", "value": "Ada", "exists": true}}) {
		t.Fatalf("MERGE INSERT RETURNING rows = %#v", result.Rows)
	}

	result, err = ExecuteSQLMutation(context.Background(), trie, `
MERGE INTO cache (key, value) VALUES ('profile:1', 'ignored')
WHEN NOT MATCHED THEN INSERT
RETURNING key, value`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLMutation(MERGE no match) error = %v", err)
	}
	if result.Affected != 0 || len(result.Rows) != 0 {
		t.Fatalf("MERGE no-match result = %#v", result)
	}
	if got := trie.GetString("profile:1"); got != "Ada" {
		t.Fatalf("profile:1 after conditional insert = %q, want Ada", got)
	}

	result, err = ExecuteSQLMutation(context.Background(), trie, `
MERGE INTO cache (key, value) VALUES ('profile:1', 'Grace')
WHEN MATCHED THEN UPDATE
RETURNING key, value`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLMutation(MERGE update) error = %v", err)
	}
	if result.Affected != 1 || !reflect.DeepEqual(result.Rows, []SQLRow{{"key": "profile:1", "value": "Grace"}}) {
		t.Fatalf("MERGE UPDATE RETURNING = %#v", result)
	}
}

func TestSQLTransactionSavepointsRollbackOnlyLaterWrites(t *testing.T) {
	trie := newTestTrie(t)
	transaction, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}
	defer transaction.Rollback()

	if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('keep', 'first')"); err != nil {
		t.Fatalf("Execute(keep) error = %v", err)
	}
	if err := transaction.Savepoint("before_discard"); err != nil {
		t.Fatalf("Savepoint() error = %v", err)
	}
	if _, err := transaction.Execute("INSERT INTO cache (key, value) VALUES ('discard', 'second')"); err != nil {
		t.Fatalf("Execute(discard) error = %v", err)
	}
	if err := transaction.RollbackTo("before_discard"); err != nil {
		t.Fatalf("RollbackTo() error = %v", err)
	}
	if err := transaction.ReleaseSavepoint("before_discard"); err != nil {
		t.Fatalf("ReleaseSavepoint() error = %v", err)
	}
	if err := transaction.RollbackTo("before_discard"); err == nil {
		t.Fatal("RollbackTo() accepted a released savepoint")
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if got := trie.GetString("keep"); got != "first" {
		t.Fatalf("keep after savepoint commit = %q, want first", got)
	}
	if trie.Exists("discard") {
		t.Fatal("discard exists after rollback to savepoint")
	}
}

func TestExecuteSQLMutationValidatesInsertSelectBeforeWriting(t *testing.T) {
	trie := newTestTrie(t)
	_, err := ExecuteSQLMutation(context.Background(), trie, `
INSERT INTO cache (key, value)
FROM VALUES ('good', 'value'), (NULL, 'invalid') AS rows(key, value)
SELECT key, value`, nil, SQLQueryOptions{})
	if err == nil {
		t.Fatal("ExecuteSQLMutation() accepted a NULL generated key")
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "good"}); !got.OK || got.Value != "0" {
		t.Fatalf("good exists after rejected INSERT SELECT = %#v, want no write", got)
	}
}

func TestSQLTransactionProvidesSnapshotReadsRollbackAndConflictVisibility(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"name":"Ada"}]`)
	tx, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('draft', 'private')"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "draft"}); !got.OK || got.Value != "0" {
		t.Fatalf("live draft = %#v, want invisible", got)
	}
	result, err := tx.Query(context.Background(), "FROM CACHE('people') AS p SELECT p.name", nil, SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("Query() = %#v, %v", result, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "draft"}); !got.OK || got.Value != "0" {
		t.Fatalf("live draft after rollback = %#v, want absent", got)
	}

	tx, err = BeginSQLTransaction(trie)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('draft', 'private')"); err != nil {
		t.Fatal(err)
	}
	trie.UpsertString("concurrent", "write")
	if err := tx.Commit(); err == nil {
		t.Fatal("Commit() accepted a stale snapshot")
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "draft"}); !got.OK || got.Value != "0" {
		t.Fatalf("live draft after conflict = %#v, want absent", got)
	}
}

func TestCompileSQLRejectsInternalReplicationCommands(t *testing.T) {
	t.Parallel()

	_, err := CompileSQL(`CALL INTERNALSET(key => 'secret', value => '{}')`)
	if err == nil || !strings.Contains(err.Error(), "internal replication command") {
		t.Fatalf("CompileSQL() error = %v, want internal-command rejection", err)
	}
}

func TestFormatSQLDiagnosticSuggestsKeywordAndShowsSourceSpan(t *testing.T) {
	t.Parallel()

	source := "SELECT value FRMO cache WHERE key = 'name'"
	_, err := CompileSQL(source)
	if err == nil {
		t.Fatal("CompileSQL() error = nil, want syntax error")
	}
	got := FormatSQLDiagnostic(source, err)
	for _, want := range []string{
		`unexpected "FRMO"; expected FROM`,
		"did you mean `FROM`?",
		"--> query:1:14",
		"SELECT value FRMO cache WHERE key = 'name'",
		"^^^^",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("FormatSQLDiagnostic() = %q, want substring %q", got, want)
		}
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}
