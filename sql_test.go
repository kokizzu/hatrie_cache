package hatriecache

import (
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
