package hatMonitoring_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"hatrie_cache/hat/hatCommand"
	"hatrie_cache/hat/hatMonitoring"
)

func TestClientCommandUsesAuthenticatedJSONContract(t *testing.T) {
	var received hatCommand.Request
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/commands" {
			t.Fatalf("request = %s %s, want POST /api/commands", request.Method, request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("authorization = %q, want bearer token", request.Header.Get("Authorization"))
		}
		if request.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("content type = %q, want application/json", request.Header.Get("Content-Type"))
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatalf("decode command request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(hatCommand.Response{OK: true, Message: "stored"}); err != nil {
			t.Fatalf("encode command response: %v", err)
		}
	}))
	defer server.Close()

	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	response, err := client.Command(context.Background(), hatCommand.Request{
		Command: "SETSTR",
		Key:     "name",
		Value:   "ivi",
	})
	if err != nil {
		t.Fatalf("Command() error = %v", err)
	}
	if !response.OK || response.Message != "stored" {
		t.Fatalf("Command() response = %#v, want successful stored response", response)
	}
	if received.Command != "SETSTR" || received.Key != "name" || received.Value != "ivi" {
		t.Fatalf("received request = %#v, want SETSTR name ivi", received)
	}
}

func TestClientCommandReportsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "command rejected", http.StatusConflict)
	}))
	defer server.Close()

	client := hatMonitoring.NewClient(server.URL, "")
	client.HTTP = server.Client()
	if _, err := client.Command(context.Background(), hatCommand.Request{Command: "SETSTR", Key: "name", Value: "ivi"}); err == nil {
		t.Fatal("Command() error = nil, want HTTP error")
	}
}
