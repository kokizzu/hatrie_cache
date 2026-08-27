package main

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestServerPublishesDiagnosticsAndCompletion(t *testing.T) {
	input := strings.Join([]string{
		lspTestFrame(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`),
		lspTestFrame(`{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{"uri":"file:///query.sql","languageId":"hatrie-sql","version":1,"text":"SELECT FROM"}}}`),
		lspTestFrame(`{"jsonrpc":"2.0","id":2,"method":"textDocument/completion","params":{"textDocument":{"uri":"file:///query.sql"},"position":{"line":0,"character":6}}}`),
		lspTestFrame(`{"jsonrpc":"2.0","id":3,"method":"shutdown"}`),
	}, "")

	var output bytes.Buffer
	server := newLSPServer(&output)
	if err := server.serve(strings.NewReader(input)); err != nil {
		t.Fatalf("serve: %v", err)
	}

	messages := lspTestMessages(t, output.Bytes())
	if len(messages) != 4 {
		t.Fatalf("message count = %d, want 4: %s", len(messages), output.String())
	}
	if messages[0]["id"] != float64(1) {
		t.Fatalf("initialize response = %#v", messages[0])
	}
	capabilities, ok := messages[0]["result"].(map[string]any)["capabilities"].(map[string]any)
	if !ok || capabilities["completionProvider"] == nil || capabilities["textDocumentSync"] == nil {
		t.Fatalf("initialize capabilities = %#v", messages[0])
	}
	if messages[1]["method"] != "textDocument/publishDiagnostics" {
		t.Fatalf("diagnostic notification = %#v", messages[1])
	}
	diagnostics := messages[1]["params"].(map[string]any)["diagnostics"].([]any)
	if len(diagnostics) == 0 || diagnostics[0].(map[string]any)["range"] == nil {
		t.Fatalf("diagnostics = %#v", messages[1])
	}
	if messages[2]["id"] != float64(2) {
		t.Fatalf("completion response = %#v", messages[2])
	}
	items := messages[2]["result"].(map[string]any)["items"].([]any)
	if len(items) == 0 || items[0].(map[string]any)["label"] == nil {
		t.Fatalf("completion items = %#v", messages[2])
	}
	if messages[3]["id"] != float64(3) {
		t.Fatalf("shutdown response = %#v", messages[3])
	}
}

func TestLSPUTF16PositionConversion(t *testing.T) {
	source := "😀x"
	if got := lspByteCharacter(source, lspPosition{Line: 0, Character: 2}); got != 4 {
		t.Fatalf("byte character = %d, want 4", got)
	}
	if got := lspUTF16Character(source, hatSql.Position{Line: 0, Character: 4}); got != 2 {
		t.Fatalf("UTF-16 character = %d, want 2", got)
	}
}

func lspTestFrame(payload string) string {
	return "Content-Length: " + strconv.Itoa(len(payload)) + "\r\n\r\n" + payload
}

func lspTestMessages(t *testing.T, bytes []byte) []map[string]any {
	t.Helper()
	frames := strings.Split(string(bytes), "Content-Length: ")
	var messages []map[string]any
	for _, frame := range frames[1:] {
		separator := strings.Index(frame, "\r\n\r\n")
		if separator < 0 {
			t.Fatalf("invalid framed response %q", frame)
		}
		var message map[string]any
		if err := json.Unmarshal([]byte(frame[separator+4:]), &message); err != nil {
			t.Fatalf("decode response %q: %v", frame, err)
		}
		messages = append(messages, message)
	}
	return messages
}
