package hatCache

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"hatrie_cache/hat/hatAuth"
	"hatrie_cache/hat/hatSql"

	"google.golang.org/grpc/metadata"
)

func tr047ObjectPolicy(command, object string) hatAuth.Policy {
	return hatAuth.Policy{
		Principals: map[string][]string{"reader-token": {"reader"}},
		Roles: []hatAuth.Role{{
			Name:  "reader",
			Rules: []hatAuth.Rule{{Commands: []string{command}, Objects: []string{object}}},
		}},
	}
}

func TestMonitoringHandlerEnforcesObjectGrantForCommands(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("tenant-a:people", `{"name":"present"}`)
	handler := NewMonitoringHandler(trie, MonitoringOptions{
		AuthToken:  "reader-token",
		RBACPolicy: tr047ObjectPolicy("GET", "tenant-a:people"),
	}).Handler()

	request := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"GET","key":"tenant-a:people"}`))
	request.Header.Set("Authorization", "Bearer reader-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("allowed object status = %d, body = %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"GET","key":"tenant-a:users"}`))
	request.Header.Set("Authorization", "Bearer reader-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("disallowed object status = %d, body = %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"BATCH","batch":[{"command":"GET","key":"tenant-a:people"},{"command":"GET","key":"tenant-a:users"}]}`))
	request.Header.Set("Authorization", "Bearer reader-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("mixed batch status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestMonitoringHandlerEnforcesObjectGrantForSQLSources(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("tenant-a:people", `{"name":"present"}`)
	handler := NewMonitoringHandler(trie, MonitoringOptions{
		AuthToken:  "reader-token",
		RBACPolicy: tr047ObjectPolicy("SQL", "tenant-a:people"),
	}).Handler()

	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"SELECT p.name FROM CACHE('tenant-a:people') AS p"}`))
	request.Header.Set("Authorization", "Bearer reader-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("allowed SQL object status = %d, body = %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"SELECT p.name FROM CACHE('tenant-a:users') AS p"}`))
	request.Header.Set("Authorization", "Bearer reader-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("disallowed SQL object status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestCacheGRPCServerEnforcesObjectGrantForCommands(t *testing.T) {
	server := NewCacheGRPCServer(newTestTrie(t), CacheGRPCOptions{
		AuthToken:  "reader-token",
		RBACPolicy: tr047ObjectPolicy("GET", "tenant-a:people"),
	})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-hatrie-auth-token", "reader-token"))

	if !server.authorizeGRPCCommand(ctx, CacheCommandRequest{Command: "GET", Key: "tenant-a:people"}) {
		t.Fatal("allowed gRPC object was denied")
	}
	if server.authorizeGRPCCommand(ctx, CacheCommandRequest{Command: "GET", Key: "tenant-a:users"}) {
		t.Fatal("disallowed gRPC object was allowed")
	}
	if server.authorizeGRPCCommand(ctx, CacheCommandRequest{Command: "BATCH", Batch: []CacheCommandRequest{
		{Command: "GET", Key: "tenant-a:people"},
		{Command: "GET", Key: "tenant-a:users"},
	}}) {
		t.Fatal("mixed gRPC batch was allowed")
	}
}

func TestMonitoringSQLRowBinaryImportFailsClosedForObjectGrant(t *testing.T) {
	payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString},
	}, []hatSql.SQLRow{{"key": "tenant-a:people", "value": "present"}})
	request := httptest.NewRequest(http.MethodPost, "/api/sql/import?query="+url.QueryEscape("INSERT INTO CACHE(key, value)"), bytes.NewReader(payload))
	request.Header.Set("Authorization", "Bearer reader-token")
	request.Header.Set("Content-Type", hatSql.SQLRowBinaryStreamContentType)
	response := httptest.NewRecorder()
	NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		AuthToken:  "reader-token",
		RBACPolicy: tr047ObjectPolicy("SQL", "tenant-a:people"),
	}).Handler().ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("object-only RowBinary import status = %d, body = %s", response.Code, response.Body.String())
	}
}
