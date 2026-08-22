package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	hatriecache "hatrie_cache"
)

func TestRunSQLCompilesAndPostsCommand(t *testing.T) {
	t.Parallel()

	var got hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/commands" {
			t.Errorf("path = %q, want /api/commands", request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&got); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true,"message":"stored string"}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	err := run(context.Background(), []string{
		"-addr", server.URL,
		"sql", "-wire-format", "json",
		"-query", "INSERT INTO cache (key, value) VALUES ('name', 'ivi')",
	}, stdout, &bytes.Buffer{}, http.DefaultClient)
	if err != nil {
		t.Fatalf("run(sql) error = %v", err)
	}
	if want := (hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("posted request = %#v, want %#v", got, want)
	}
	if got := stdout.String(); got != "{\"ok\":true,\"message\":\"stored string\"}\n" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestRunSQLReturnsCompilerDiagnosticBeforeHTTP(t *testing.T) {
	t.Parallel()

	err := run(context.Background(), []string{
		"sql", "-query", "SELECT value FRMO cache WHERE key = 'name'",
	}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil {
		t.Fatal("run(sql) error = nil, want compiler diagnostic")
	}
	for _, want := range []string{"did you mean `FROM`?", "--> query:1:14"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("run(sql) error = %q, want substring %q", err, want)
		}
	}
}

func TestRunSQLPostsRelationalQueryToSQLRoute(t *testing.T) {
	t.Parallel()

	var got hatriecache.SQLQueryRequest
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/sql" {
			t.Errorf("path = %q, want /api/sql", request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&got); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"columns":["name"],"rows":[{"name":"Ivi"}]}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	query := "FROM VALUES ('Ivi') AS people(name) SELECT name"
	if err := run(context.Background(), []string{"-addr", server.URL, "sql", "-query", query}, stdout, &bytes.Buffer{}, http.DefaultClient); err != nil {
		t.Fatalf("run(sql relational) error = %v", err)
	}
	if got.Query != query {
		t.Fatalf("posted query = %q, want %q", got.Query, query)
	}
}

func TestRunSQLPostsGoFunctionToSQLFunctionRoute(t *testing.T) {
	t.Parallel()

	var got hatriecache.SQLFunctionDefinition
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/sql/functions" {
			t.Errorf("path = %q, want /api/sql/functions", request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&got); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	query := "CREATE FUNCTION eligible(age INTEGER) LANGUAGE GO AS 'return age > 10'"
	if err := run(context.Background(), []string{"-addr", server.URL, "sql", "-query", query}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient); err != nil {
		t.Fatalf("run(sql function) error = %v", err)
	}
	if want := (hatriecache.SQLFunctionDefinition{Name: "eligible", Arguments: []string{"age"}, ArgumentTypes: []string{"INTEGER"}, Language: "GO", Source: "return age > 10"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("posted definition = %#v, want %#v", got, want)
	}
}
