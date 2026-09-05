package hatCache

import (
	"encoding/json"
	"testing"
)

func TestCommandErrorCodes(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{name: "invalid argument", message: "key is required", want: "invalid_argument"},
		{name: "unsupported command", message: "unsupported command", want: "unsupported_command"},
		{name: "counter overflow", message: "counter overflow", want: "counter_overflow"},
		{name: "internal", message: "storage engine unavailable", want: "internal_error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := commandError(test.message)
			if response.Code != test.want {
				t.Fatalf("commandError(%q).Code = %q, want %q", test.message, response.Code, test.want)
			}
		})
	}
}

func TestCommandErrorIncludesStableCode(t *testing.T) {
	trie := newTestTrie(t)
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "GET"})
	if response.OK {
		t.Fatalf("response = %#v, want command failure", response)
	}
	if response.Message != "key is required" {
		t.Fatalf("response.Message = %q, want legacy message", response.Message)
	}

	payload, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var wire struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if wire.Code != "invalid_argument" {
		t.Fatalf("wire code = %q, want invalid_argument; payload = %s", wire.Code, payload)
	}
	if wire.Message != response.Message {
		t.Fatalf("wire message = %q, want %q", wire.Message, response.Message)
	}
}

func BenchmarkCommandErrorResponseJSON(b *testing.B) {
	response := commandError("key is required")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(response); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCommandSuccessResponseJSON(b *testing.B) {
	response := CacheCommandResponse{OK: true, Message: "ok", Value: "value"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(response); err != nil {
			b.Fatal(err)
		}
	}
}
