package hatMonitoring_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"hatrie_cache/hat/hatMonitoring"
)

func TestClientFetchesHealthAndEntries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer secret" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch request.URL.Path {
		case "/api/health":
			_, _ = writer.Write([]byte(`{"status":"ok","node":"sg-1","api_version":1}`))
		case "/api/entries":
			if request.URL.Query().Get("prefix") != "user:" || request.URL.Query().Get("limit") != "2" {
				t.Errorf("entries query = %q", request.URL.RawQuery)
			}
			_, _ = writer.Write([]byte(`{"entries":[{"key":"user:1","type":"string"}],"limit":2}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := hatMonitoring.NewClient(server.URL, "secret")
	health, err := client.Health(context.Background())
	if err != nil || health.Node != "sg-1" {
		t.Fatalf("Health() = %#v, %v", health, err)
	}
	entries, err := client.Entries(context.Background(), hatMonitoring.EntriesRequest{Prefix: "user:", Limit: 2})
	if err != nil || len(entries.Entries) != 1 || entries.Entries[0].Key != "user:1" {
		t.Fatalf("Entries() = %#v, %v", entries, err)
	}
}
