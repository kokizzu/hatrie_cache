package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	hatriecache "hatrie_cache"
)

func TestSQLREPLExecutesMultilineStatementAndDescribe(t *testing.T) {
	queries := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/sql" {
			t.Fatalf("request = %s %s, want POST /api/sql", request.Method, request.URL.Path)
		}
		var payload hatriecache.SQLQueryRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		queries = append(queries, payload.Query)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	historyPath := t.TempDir() + "/history.json"
	var stdout, stderr bytes.Buffer
	err := runSQLREPL(context.Background(), server.Client(), server.URL, strings.NewReader("SELECT name\nFROM CACHE('users');\n\\describe CACHE('users')\n\\q\n"), &stdout, &stderr, historyPath)
	if err != nil {
		t.Fatalf("runSQLREPL() error = %v", err)
	}
	if got, want := queries, []string{"SELECT name\nFROM CACHE('users')", "EXPLAIN SELECT * FROM CACHE('users')"}; !equalStrings(got, want) {
		t.Fatalf("queries = %#v, want %#v", got, want)
	}
	data, err := os.ReadFile(historyPath)
	if err != nil {
		t.Fatalf("ReadFile(history) error = %v", err)
	}
	if !strings.Contains(string(data), "SELECT name\\nFROM CACHE") {
		t.Fatalf("history = %s, want stored multiline query", data)
	}
}

func TestSQLREPLCompletionAndHistory(t *testing.T) {
	historyPath := t.TempDir() + "/history.json"
	var stdout, stderr bytes.Buffer
	err := runSQLREPL(context.Background(), http.DefaultClient, "http://unused.test", strings.NewReader("\\complete SELECT name \n\\history\n\\q\n"), &stdout, &stderr, historyPath)
	if err != nil {
		t.Fatalf("runSQLREPL() error = %v", err)
	}
	if !strings.Contains(stdout.String(), "FROM") {
		t.Fatalf("completion output = %q, want FROM", stdout.String())
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
