package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	hatriecache "hatrie_cache"
	"hatrie_cache/internal/gen/hatriecache/v1"
	"hatrie_cache/internal/jsonwire"

	"google.golang.org/protobuf/proto"
)

type cliRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn cliRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type trackingResponseBody struct {
	reader *strings.Reader
	closed bool
	eof    bool
	read   int
}

func newTrackingResponseBody(value string) *trackingResponseBody {
	return &trackingResponseBody{reader: strings.NewReader(value)}
}

func (body *trackingResponseBody) Read(data []byte) (int, error) {
	n, err := body.reader.Read(data)
	body.read += n
	if err == io.EOF {
		body.eof = true
	}
	return n, err
}

func (body *trackingResponseBody) Close() error {
	body.closed = true
	return nil
}

func TestRunRequiresSubcommand(t *testing.T) {
	err := run(context.Background(), nil, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "subcommand is required") {
		t.Fatalf("run() error = %v, want subcommand error", err)
	}
}

func TestRunAcceptsNilWriters(t *testing.T) {
	err := run(context.Background(), nil, nil, nil, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "subcommand is required") {
		t.Fatalf("run(nil writers) error = %v, want subcommand error", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"online"}`))
	}))
	defer server.Close()

	if err := run(nil, []string{"-addr", server.URL, "health"}, nil, nil, nil); err != nil {
		t.Fatalf("run(health, nil writers) error = %v", err)
	}
}

func TestRunDefaultsNilContextAndClient(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"online"}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(nil, []string{"-addr", server.URL, "health"}, stdout, &bytes.Buffer{}, nil); err != nil {
		t.Fatalf("run(nil context/client) error = %v", err)
	}
	if gotPath != "/api/health" {
		t.Fatalf("path = %q, want /api/health", gotPath)
	}
	if got := stdout.String(); got != "{\"status\":\"online\"}\n" {
		t.Fatalf("stdout = %q, want health JSON with trailing newline", got)
	}
}

func TestAuthenticatedHTTPClientDoesNotForwardTokenAcrossOriginRedirect(t *testing.T) {
	var destinationAuthorization string
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		destinationAuthorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer destination.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL+"/redirected", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	client := authenticatedHTTPClient(origin.Client(), "operator-secret")
	response, err := client.Get(origin.URL + "/start")
	if err != nil {
		t.Fatalf("Get(cross-origin redirect) error = %v", err)
	}
	drainAndCloseResponse(response.Body)
	if destinationAuthorization != "" {
		t.Fatalf("redirected Authorization = %q, want empty", destinationAuthorization)
	}
}

func TestAuthenticatedHTTPClientKeepsTokenOnSameOriginRedirect(t *testing.T) {
	var redirectedAuthorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
			return
		}
		redirectedAuthorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := authenticatedHTTPClient(server.Client(), "operator-secret")
	response, err := client.Get(server.URL + "/start")
	if err != nil {
		t.Fatalf("Get(same-origin redirect) error = %v", err)
	}
	drainAndCloseResponse(response.Body)
	if redirectedAuthorization != "Bearer operator-secret" {
		t.Fatalf("redirected Authorization = %q, want bearer token", redirectedAuthorization)
	}
}

func BenchmarkAuthenticatedHTTPClientRequest(b *testing.B) {
	response := &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}
	base := &http.Client{Transport: cliRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		return response, nil
	})}
	client := authenticatedHTTPClient(base, "operator-secret")
	request, err := http.NewRequest(http.MethodGet, "https://node-a.example/api/health", nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := client.Transport.RoundTrip(request); err != nil {
			b.Fatal(err)
		}
	}
}

func TestHTTPHelpersAcceptNilStdout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/health":
			w.Write([]byte(`{"status":"online"}`))
		case "/api/commands":
			w.Write([]byte(`{"ok":true,"message":"ok"}`))
		default:
			t.Fatalf("unexpected path = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	if err := getJSON(nil, nil, server.URL, "/api/health", nil); err != nil {
		t.Fatalf("getJSON(nil stdout) error = %v", err)
	}
	if err := postCommandValue(nil, nil, server.URL, hatriecache.CacheCommandRequest{Command: "GET", Key: "name"}, "json", nil); err != nil {
		t.Fatalf("postCommandValue(nil stdout) error = %v", err)
	}
}

func TestHTTPHelpersDefaultNilContextAndClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/health":
			if r.Method != http.MethodGet {
				t.Fatalf("health method = %s, want GET", r.Method)
			}
			w.Write([]byte(`{"status":"online"}`))
		case "/api/replication":
			if r.Method != http.MethodPost {
				t.Fatalf("replication method = %s, want POST", r.Method)
			}
			w.Write([]byte(`{"ok":true,"message":"replicated"}`))
		case "/api/commands":
			if r.Method != http.MethodPost {
				t.Fatalf("command method = %s, want POST", r.Method)
			}
			w.Write([]byte(`{"ok":true,"message":"ok"}`))
		case "/api/topology":
			if r.Method != http.MethodPut {
				t.Fatalf("topology method = %s, want PUT", r.Method)
			}
			w.Write([]byte(`{"ok":true,"message":"updated"}`))
		default:
			t.Fatalf("unexpected path = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	for name, call := range map[string]func(*bytes.Buffer) error{
		"get": func(stdout *bytes.Buffer) error {
			return getJSON(nil, nil, server.URL, "/api/health", stdout)
		},
		"post-json": func(stdout *bytes.Buffer) error {
			return postJSON(nil, nil, server.URL, "/api/replication", []byte(`{"prefix":"session:"}`), stdout)
		},
		"post-command": func(stdout *bytes.Buffer) error {
			return postCommandValue(nil, nil, server.URL, hatriecache.CacheCommandRequest{Command: "GET", Key: "name"}, "json", stdout)
		},
		"put-json": func(stdout *bytes.Buffer) error {
			return putJSONReader(nil, nil, server.URL, "/api/topology", strings.NewReader(`{"nodes":[]}`), stdout)
		},
	} {
		t.Run(name, func(t *testing.T) {
			stdout := &bytes.Buffer{}
			if err := call(stdout); err != nil {
				t.Fatalf("%s helper error = %v", name, err)
			}
			if !strings.HasSuffix(stdout.String(), "\n") {
				t.Fatalf("%s stdout = %q, want trailing newline", name, stdout.String())
			}
		})
	}
}

func TestParseGlobalFlagsConfiguresTimeout(t *testing.T) {
	t.Setenv("HATRIE_CACHE_AUTH_TOKEN", "")
	cfg, remaining, err := parseGlobalFlags([]string{"-addr", "http://cache", "-timeout", "250ms", "-token", "secret", "stats"}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseGlobalFlags() error = %v", err)
	}
	if cfg.addr != "http://cache" {
		t.Fatalf("addr = %q, want http://cache", cfg.addr)
	}
	if cfg.timeout != 250*time.Millisecond {
		t.Fatalf("timeout = %s, want 250ms", cfg.timeout)
	}
	if cfg.token != "secret" {
		t.Fatalf("token = %q, want secret", cfg.token)
	}
	if !reflect.DeepEqual(remaining, []string{"stats"}) {
		t.Fatalf("remaining = %#v, want stats", remaining)
	}

	cfg, _, err = parseGlobalFlags([]string{"-timeout", "0", "stats"}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseGlobalFlags(timeout 0) error = %v", err)
	}
	if cfg.timeout != 0 {
		t.Fatalf("timeout disabled = %s, want 0", cfg.timeout)
	}

	t.Setenv("HATRIE_CACHE_AUTH_TOKEN", "env-secret")
	cfg, _, err = parseGlobalFlags([]string{"stats"}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseGlobalFlags(env token) error = %v", err)
	}
	if cfg.token != "env-secret" {
		t.Fatalf("env token = %q, want env-secret", cfg.token)
	}
}

func TestRunAppliesRequestTimeout(t *testing.T) {
	var gotTimeout bool
	client := &http.Client{Transport: cliRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		deadline, ok := request.Context().Deadline()
		if !ok {
			t.Fatal("request context has no deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > time.Second {
			t.Fatalf("request deadline remaining = %s, want within configured timeout", remaining)
		}
		gotTimeout = true
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(`{"status":"online"}`)),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Request:    request,
		}, nil
	})}

	if err := run(context.Background(), []string{"-timeout", "1s", "health"}, &bytes.Buffer{}, &bytes.Buffer{}, client); err != nil {
		t.Fatalf("run(health) error = %v", err)
	}
	if !gotTimeout {
		t.Fatal("test transport was not called")
	}
}

func TestRunPassesAuthToken(t *testing.T) {
	var gotAuthorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"online"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{"-addr", server.URL, "-token", "secret", "health"}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(health -token) error = %v", err)
	}
	if gotAuthorization != "Bearer secret" {
		t.Fatalf("Authorization = %q, want bearer token", gotAuthorization)
	}
}

func TestRunAllowsDisablingRequestTimeout(t *testing.T) {
	client := &http.Client{Transport: cliRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if _, ok := request.Context().Deadline(); ok {
			t.Fatal("request context has deadline with -timeout 0")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(`{"status":"online"}`)),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Request:    request,
		}, nil
	})}

	if err := run(context.Background(), []string{"-timeout", "0", "health"}, &bytes.Buffer{}, &bytes.Buffer{}, client); err != nil {
		t.Fatalf("run(health -timeout 0) error = %v", err)
	}
}

func TestRunRejectsNegativeTimeout(t *testing.T) {
	err := run(context.Background(), []string{"-timeout", "-1s", "health"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "timeout must be non-negative") {
		t.Fatalf("run(negative timeout) error = %v, want non-negative timeout error", err)
	}
}

func TestRunFetchesStats(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"reads":7}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", server.URL, "stats"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(stats) error = %v", err)
	}
	if gotPath != "/api/stats" {
		t.Fatalf("path = %q, want /api/stats", gotPath)
	}
	if got := stdout.String(); got != "{\"reads\":7}\n" {
		t.Fatalf("stdout = %q, want stats JSON", got)
	}
}

func TestRunDoesNotDuplicateTrailingNewline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{\"reads\":7}\n"))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", server.URL, "stats"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(stats) error = %v", err)
	}
	if got := stdout.String(); got != "{\"reads\":7}\n" {
		t.Fatalf("stdout = %q, want exactly one trailing newline", got)
	}
}

func TestRunBoundsErrorResponseBody(t *testing.T) {
	payload := strings.Repeat("x", maxErrorBodyBytes+128) + "tail-marker"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(payload))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	err := run(context.Background(), []string{"-addr", server.URL, "stats"}, stdout, &bytes.Buffer{}, server.Client())
	if err == nil || !strings.Contains(err.Error(), "server returned 500 Internal Server Error") {
		t.Fatalf("run(stats) error = %v, want server error", err)
	}
	if strings.Contains(err.Error(), "tail-marker") {
		t.Fatalf("error included body beyond limit")
	}
	if !strings.Contains(err.Error(), truncatedErrorBodySuffix) {
		t.Fatalf("error = %q, want truncation suffix", err)
	}
	if len(err.Error()) > maxErrorBodyBytes+128 {
		t.Fatalf("error length = %d, want bounded body", len(err.Error()))
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout len = %d, want empty on server error", stdout.Len())
	}
}

func TestDoAndCopyDrainsAndClosesErrorResponse(t *testing.T) {
	payload := strings.Repeat("x", maxErrorBodyBytes+128)
	body := newTrackingResponseBody(payload)
	client := &http.Client{Transport: cliRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Status:     "500 Internal Server Error",
			Body:       body,
			Header:     make(http.Header),
			Request:    request,
		}, nil
	})}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://example.invalid/stats", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	stdout := &bytes.Buffer{}
	err = doAndCopy(client, request, stdout)
	if err == nil || !strings.Contains(err.Error(), "500 Internal Server Error") {
		t.Fatalf("doAndCopy() error = %v, want server error", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout len = %d, want empty on server error", stdout.Len())
	}
	if !body.eof {
		t.Fatal("response body was not drained to EOF")
	}
	if !body.closed {
		t.Fatal("response body was not closed")
	}
	if body.read != len(payload) {
		t.Fatalf("response body read = %d, want %d", body.read, len(payload))
	}
}

func TestRunEntriesPassesPrefix(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		w.Write([]byte(`{"entries":[]}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{"-addr", server.URL, "entries", "-prefix", "session:"}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(entries) error = %v", err)
	}
	if gotPath != "/api/entries?prefix=session%3A" {
		t.Fatalf("path = %q, want prefix query", gotPath)
	}
}

func TestRunEntriesPassesLimit(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		w.Write([]byte(`{"entries":[],"limit":2}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{"-addr", server.URL, "entries", "-prefix", "session:", "-limit", "2", "-after-key", "session:2"}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(entries -limit) error = %v", err)
	}
	if gotPath != "/api/entries?after_key=session%3A2&limit=2&prefix=session%3A" {
		t.Fatalf("path = %q, want prefix, limit, and cursor query", gotPath)
	}
}

func TestRunStorageCompactPostsRange(t *testing.T) {
	var gotPath string
	var gotMethod string
	var gotRequest struct {
		StartKey string `json:"start_key"`
		LimitKey string `json:"limit_key"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		gotMethod = r.Method
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode(storage compact request) error = %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"store":"leveldb","start_key":"alpha","limit_key":"omega"}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", server.URL, "storage", "compact", "-start-key", "alpha", "-limit-key", "omega"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(storage compact) error = %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/api/storage/compact" {
		t.Fatalf("storage compact method/path = %s %s, want POST /api/storage/compact", gotMethod, gotPath)
	}
	if gotRequest.StartKey != "alpha" || gotRequest.LimitKey != "omega" {
		t.Fatalf("storage compact request = %#v, want range", gotRequest)
	}
	if !strings.Contains(stdout.String(), `"store":"leveldb"`) {
		t.Fatalf("stdout = %q, want storage response", stdout.String())
	}
}

func TestRunStorageFlushPosts(t *testing.T) {
	var gotPath string
	var gotMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		gotMethod = r.Method
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"store":"leveldb","keys":1}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", server.URL, "storage", "flush"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(storage flush) error = %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/api/storage/flush" {
		t.Fatalf("storage flush method/path = %s %s, want POST /api/storage/flush", gotMethod, gotPath)
	}
	if !strings.Contains(stdout.String(), `"keys":1`) {
		t.Fatalf("stdout = %q, want flush response", stdout.String())
	}
}

func TestRunTopologyGetsAndRoutes(t *testing.T) {
	var gotPaths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPaths = append(gotPaths, r.URL.String())
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		w.Write([]byte(`{"version":1,"self":"node-a","nodes":[{"id":"node-a"}],"shards":[{"id":0,"primary":"node-a"}]}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{"-addr", server.URL, "topology"}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(topology) error = %v", err)
	}
	if err := run(context.Background(), []string{"-addr", server.URL, "topology", "-key", "session:1"}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(topology -key) error = %v", err)
	}
	if len(gotPaths) != 2 || gotPaths[0] != "/api/topology" || gotPaths[1] != "/api/topology?key=session%3A1" {
		t.Fatalf("paths = %#v, want topology and route paths", gotPaths)
	}
}

func TestRunTopologyUploadsFile(t *testing.T) {
	var gotMethod string
	var gotTopology hatriecache.ClusterTopology
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotTopology); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"version":1}`))
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "topology.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"self":"node-a","nodes":[{"id":"node-a"}],"shards":[{"id":0,"primary":"node-a"}]}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := run(context.Background(), []string{"-addr", server.URL, "topology", "-file", path}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(topology -file) error = %v", err)
	}
	if gotMethod != http.MethodPut || gotTopology.Self != "node-a" {
		t.Fatalf("request = %s %#v, want PUT node-a topology", gotMethod, gotTopology)
	}
}

func TestPutJSONReaderSendsStreamingBody(t *testing.T) {
	var gotBody struct {
		Version int `json:"version"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	body := iotest.OneByteReader(strings.NewReader(`{"version":1}`))
	if err := putJSONReader(context.Background(), server.Client(), server.URL, "/api/topology", body, stdout); err != nil {
		t.Fatalf("putJSONReader() error = %v", err)
	}
	if gotBody.Version != 1 {
		t.Fatalf("body = %#v, want version 1", gotBody)
	}
	if got := stdout.String(); got != "{\"ok\":true}\n" {
		t.Fatalf("stdout = %q, want response JSON", got)
	}
}

func TestRunElectionGetsRoutesAndUpdatesNodes(t *testing.T) {
	var gotRequests []string
	var updates []struct {
		Node   string `json:"node"`
		Online bool   `json:"online"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequests = append(gotRequests, r.Method+" "+r.URL.String())
		if r.Method == http.MethodPost {
			var update struct {
				Node   string `json:"node"`
				Online bool   `json:"online"`
			}
			if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			updates = append(updates, update)
		}
		w.Write([]byte(`{"leaders":[]}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{"-addr", server.URL, "election"}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(election) error = %v", err)
	}
	if err := run(context.Background(), []string{"-addr", server.URL, "election", "-key", "session:1"}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(election -key) error = %v", err)
	}
	if err := run(context.Background(), []string{"-addr", server.URL, "election", "-heartbeat", "node-a"}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(election -heartbeat) error = %v", err)
	}
	if err := run(context.Background(), []string{"-addr", server.URL, "election", "-offline", "node-b"}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(election -offline) error = %v", err)
	}

	wantRequests := []string{
		"GET /api/election",
		"GET /api/election?key=session%3A1",
		"POST /api/election",
		"POST /api/election",
	}
	if !reflect.DeepEqual(gotRequests, wantRequests) {
		t.Fatalf("requests = %#v, want %#v", gotRequests, wantRequests)
	}
	if len(updates) != 2 || updates[0].Node != "node-a" || !updates[0].Online || updates[1].Node != "node-b" || updates[1].Online {
		t.Fatalf("updates = %#v, want node-a online then node-b offline", updates)
	}
}

func TestRunElectionRejectsConflictingFlags(t *testing.T) {
	err := run(context.Background(), []string{"election", "-key", "k", "-offline", "node-a"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("run(election conflicting flags) error = %v, want mutually exclusive", err)
	}
}

func TestRunReplicationFetchesStatus(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		w.Write([]byte(`{"skipped":true,"reason":"none","dead_letter_count":1,"dead_letters":[{"id":1,"key":"session:1","attempts":2}]}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", server.URL, "replication"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(replication) error = %v", err)
	}
	if gotPath != "/api/replication" {
		t.Fatalf("path = %q, want /api/replication", gotPath)
	}
	if got := stdout.String(); got != "{\"skipped\":true,\"reason\":\"none\",\"dead_letter_count\":1,\"dead_letters\":[{\"id\":1,\"key\":\"session:1\",\"attempts\":2}]}\n" {
		t.Fatalf("stdout = %q, want replication JSON", got)
	}
}

func TestRunReplicationSyncPostsPrefix(t *testing.T) {
	var gotPath string
	var gotBody struct {
		Prefix string `json:"prefix"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"command":"SYNC","key":"session:","entries":1,"skipped":false}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", server.URL, "replication", "-sync", "-prefix", "session:"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(replication -sync) error = %v", err)
	}
	if gotPath != "/api/replication" {
		t.Fatalf("path = %q, want /api/replication", gotPath)
	}
	if gotBody.Prefix != "session:" {
		t.Fatalf("prefix = %q, want session:", gotBody.Prefix)
	}
	if got := stdout.String(); got != "{\"command\":\"SYNC\",\"key\":\"session:\",\"entries\":1,\"skipped\":false}\n" {
		t.Fatalf("stdout = %q, want sync JSON", got)
	}
}

func TestRunReplicationRejectsPrefixWithoutSync(t *testing.T) {
	err := run(context.Background(), []string{"replication", "-prefix", "session:"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "requires -sync") {
		t.Fatalf("run(replication -prefix) error = %v, want requires -sync", err)
	}
}

func TestRunJournalFetchesTail(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		w.Write([]byte(`{"last_sequence":8,"entries":[{"sequence":8,"request":{"command":"INC","key":"views"}}]}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", server.URL, "journal", "-after-sequence", "7", "-limit", "25"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(journal) error = %v", err)
	}
	if gotPath != "/api/journal?after_sequence=7&limit=25" {
		t.Fatalf("path = %q, want /api/journal?after_sequence=7&limit=25", gotPath)
	}
	if got := stdout.String(); got != "{\"last_sequence\":8,\"entries\":[{\"sequence\":8,\"request\":{\"command\":\"INC\",\"key\":\"views\"}}]}\n" {
		t.Fatalf("stdout = %q, want journal JSON", got)
	}
}

func TestRunJournalPullPostsSource(t *testing.T) {
	var gotPath string
	var gotBody struct {
		Source        string `json:"source"`
		AfterSequence uint64 `json:"after_sequence"`
		Limit         uint64 `json:"limit"`
		UntilCurrent  bool   `json:"until_current"`
		MaxBatches    uint64 `json:"max_batches"`
		WireFormat    string `json:"wire_format"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"source":"http://leader","after_sequence":7,"last_sequence":8,"applied":1,"applied_through":8}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"journal",
		"-pull-from", " http://leader ",
		"-after-sequence", "7",
		"-limit", "25",
		"-until-current",
		"-max-batches", "3",
		"-wire-format", "json",
	}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(journal -pull-from) error = %v", err)
	}
	if gotPath != "/api/journal" {
		t.Fatalf("path = %q, want /api/journal", gotPath)
	}
	if gotBody.Source != "http://leader" || gotBody.AfterSequence != 7 || gotBody.Limit != 25 || !gotBody.UntilCurrent || gotBody.MaxBatches != 3 || gotBody.WireFormat != "json" {
		t.Fatalf("body = %#v, want source http://leader after 7 limit 25 until current max batches 3", gotBody)
	}
	if got := stdout.String(); got != "{\"source\":\"http://leader\",\"after_sequence\":7,\"last_sequence\":8,\"applied\":1,\"applied_through\":8}\n" {
		t.Fatalf("stdout = %q, want journal pull JSON", got)
	}
}

func TestRunJournalRejectsInvalidFollowFlags(t *testing.T) {
	err := run(context.Background(), []string{"journal", "-until-current"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "requires -pull-from") {
		t.Fatalf("run(journal -until-current) error = %v, want requires -pull-from", err)
	}
	err = run(context.Background(), []string{"journal", "-max-batches", "3"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "requires -until-current") {
		t.Fatalf("run(journal -max-batches) error = %v, want requires -until-current", err)
	}
	err = run(context.Background(), []string{"journal", "-wire-format", "protobuf"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "wire format") {
		t.Fatalf("run(journal invalid wire format) error = %v", err)
	}
}

func TestRunCommandPostsProtobufByDefault(t *testing.T) {
	var gotRequest hatriecachev1.CommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/x-protobuf" {
			t.Fatalf("Content-Type = %q, want application/x-protobuf", got)
		}
		if got := r.Header.Get("Accept"); got != "application/x-protobuf" {
			t.Fatalf("Accept = %q, want application/x-protobuf", got)
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if err := proto.Unmarshal(data, &gotRequest); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}
		response, err := proto.Marshal(&hatriecachev1.CommandResponse{Ok: true, Message: "stored"})
		if err != nil {
			t.Fatalf("Marshal(response) error = %v", err)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(response)
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-cmd", "SETSTR",
		"-key", "name",
		"-value", "ivi",
		"-ttl-seconds", "60",
	}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command) error = %v", err)
	}
	if gotRequest.GetCommand() != "SETSTR" || gotRequest.GetKey() != "name" || gotRequest.GetValue() != "ivi" {
		t.Fatalf("request = %q/%q/%q, want SETSTR/name/ivi", gotRequest.GetCommand(), gotRequest.GetKey(), gotRequest.GetValue())
	}
	if gotRequest.TtlSeconds == nil || gotRequest.GetTtlSeconds() != 60 {
		t.Fatalf("ttl = %v, want 60", gotRequest.TtlSeconds)
	}
	if got := stdout.String(); got != "{\"ok\":true,\"message\":\"stored\"}\n" {
		t.Fatalf("stdout = %q, want command response", got)
	}
}

func TestRunCommandCopiesSuccessfulUnsupportedResponseContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept"); got != "application/x-protobuf" {
			t.Fatalf("Accept = %q, want application/x-protobuf", got)
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("legacy command ok"))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-cmd", "GET",
		"-key", "name",
	}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command legacy response) error = %v", err)
	}
	if got := stdout.String(); got != "legacy command ok\n" {
		t.Fatalf("stdout = %q, want raw legacy response", got)
	}
}

func TestRunCommandAutoRetriesJSONWhenServerRejectsProtobuf(t *testing.T) {
	var attempts int32
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := atomic.AddInt32(&attempts, 1)
		switch attempt {
		case 1:
			if got := r.Header.Get("Content-Type"); got != "application/x-protobuf" {
				t.Fatalf("first Content-Type = %q, want application/x-protobuf", got)
			}
			http.Error(w, "unsupported command request content type", http.StatusUnsupportedMediaType)
		case 2:
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Fatalf("retry Content-Type = %q, want application/json", got)
			}
			if got := r.Header.Get("Accept"); got != "application/json" {
				t.Fatalf("retry Accept = %q, want application/json", got)
			}
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("Decode(retry) error = %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"ok":true,"message":"stored after retry"}`))
		default:
			t.Fatalf("unexpected attempt %d", attempt)
		}
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-cmd", "SETSTR",
		"-key", "name",
		"-value", "ivi",
	}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command retry) error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if gotRequest.Command != "SETSTR" || gotRequest.Key != "name" || gotRequest.Value != "ivi" {
		t.Fatalf("retry request = %#v, want SETSTR name ivi", gotRequest)
	}
	if got := stdout.String(); got != "{\"ok\":true,\"message\":\"stored after retry\"}\n" {
		t.Fatalf("stdout = %q, want retry command response", got)
	}
}

func TestRunCommandForcedProtobufDoesNotRetryJSON(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		if got := r.Header.Get("Content-Type"); got != "application/x-protobuf" {
			t.Fatalf("Content-Type = %q, want application/x-protobuf", got)
		}
		http.Error(w, "unsupported command request content type", http.StatusUnsupportedMediaType)
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "protobuf",
		"-cmd", "SETSTR",
		"-key", "name",
		"-value", "ivi",
	}, stdout, &bytes.Buffer{}, server.Client())
	if err == nil || !strings.Contains(err.Error(), "415 Unsupported Media Type") {
		t.Fatalf("run(command forced protobuf) error = %v, want 415", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout len = %d, want empty", stdout.Len())
	}
}

func TestRunCommandReportsProtobufErrorResponseAsJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept"); got != "application/x-protobuf" {
			t.Fatalf("Accept = %q, want application/x-protobuf", got)
		}
		response, err := proto.Marshal(&hatriecachev1.CommandResponse{Ok: false, Message: "not leader"})
		if err != nil {
			t.Fatalf("Marshal(response) error = %v", err)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusConflict)
		w.Write(response)
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-cmd", "SETSTR",
		"-key", "name",
		"-value", "ivi",
	}, stdout, &bytes.Buffer{}, server.Client())
	if err == nil || !strings.Contains(err.Error(), `server returned 409 Conflict: {"ok":false,"message":"not leader"}`) {
		t.Fatalf("run(command conflict) error = %v, want decoded protobuf command response", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout len = %d, want empty on command error", stdout.Len())
	}
}

func TestRunCommandMarksTruncatedErrorResponseBody(t *testing.T) {
	payload := strings.Repeat("x", maxErrorBodyBytes+128) + "tail-marker"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(payload))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-cmd", "SETSTR",
		"-key", "name",
		"-value", "ivi",
	}, stdout, &bytes.Buffer{}, server.Client())
	if err == nil || !strings.Contains(err.Error(), "server returned 409 Conflict") {
		t.Fatalf("run(command conflict) error = %v, want conflict", err)
	}
	if strings.Contains(err.Error(), "tail-marker") {
		t.Fatalf("command error included body beyond limit")
	}
	if !strings.Contains(err.Error(), truncatedErrorBodySuffix) {
		t.Fatalf("command error = %q, want truncation suffix", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout len = %d, want empty on command error", stdout.Len())
	}
}

func TestRunCommandPostsJSONWhenRequested(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q, want application/json", got)
		}
		if got := r.Header.Get("Content-Encoding"); got != "" {
			t.Fatalf("Content-Encoding = %q, want empty for small command", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"stored"}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "SETSTR",
		"-key", "name",
		"-value", "ivi",
		"-ttl-seconds", "60",
	}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command) error = %v", err)
	}
	if gotRequest.Command != "SETSTR" || gotRequest.Key != "name" || gotRequest.Value != "ivi" {
		t.Fatalf("request = %#v, want SETSTR name ivi", gotRequest)
	}
	if gotRequest.TTLSeconds == nil || *gotRequest.TTLSeconds != 60 {
		t.Fatalf("ttl = %v, want 60", gotRequest.TTLSeconds)
	}
	if got := stdout.String(); got != "{\"ok\":true,\"message\":\"stored\"}\n" {
		t.Fatalf("stdout = %q, want command response", got)
	}
}

func TestRunCommandCompressesLargeJSONPost(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Encoding"); got != "gzip" {
			t.Fatalf("Content-Encoding = %q, want gzip", got)
		}
		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Fatalf("NewReader() error = %v", err)
		}
		defer reader.Close()
		if err := json.NewDecoder(reader).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"stored"}`))
	}))
	defer server.Close()

	large := strings.Repeat("x", minCompressedJSONRequestBytes)
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "SETSTR",
		"-key", "large",
		"-value", large,
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command large) error = %v", err)
	}
	if gotRequest.Command != "SETSTR" || gotRequest.Key != "large" || gotRequest.Value != large {
		t.Fatalf("request = %#v, want large SETSTR", gotRequest)
	}
}

func TestCommandRequestBodyCompressesEscapedLargeJSONValue(t *testing.T) {
	payload := hatriecache.CacheCommandRequest{
		Command: "SETSTR",
		Key:     "escaped",
		Value:   strings.Repeat("\n", minCompressedJSONRequestBytes/2),
	}
	body, contentType, contentEncoding, err := commandRequestBody(payload, "json")
	if err != nil {
		t.Fatalf("commandRequestBody(escaped value) error = %v", err)
	}
	if contentType != "application/json" || contentEncoding != "gzip" {
		t.Fatalf("content type/encoding = %q/%q, want JSON/gzip", contentType, contentEncoding)
	}
	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(compressed body) error = %v", err)
	}
	defer reader.Close()
	var decoded hatriecache.CacheCommandRequest
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(compressed escaped body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded escaped payload = %#v, want %#v", decoded, payload)
	}
}

func TestJSONRequestBodyLeavesSmallBodyPlain(t *testing.T) {
	payload := []byte(`{"ok":true}`)
	body, contentEncoding, err := jsonRequestBody(payload)
	if err != nil {
		t.Fatalf("jsonRequestBody() error = %v", err)
	}
	if contentEncoding != "" {
		t.Fatalf("Content-Encoding = %q, want empty", contentEncoding)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll(plain body) error = %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("plain body = %q, want %q", data, payload)
	}
}

func TestJSONRequestBodyCompressesLargeBody(t *testing.T) {
	payload := []byte(`{"value":"` + strings.Repeat("x", minCompressedJSONRequestBytes) + `"}`)
	body, contentEncoding, err := jsonRequestBody(payload)
	if err != nil {
		t.Fatalf("jsonRequestBody() error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(compressed body) error = %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll(compressed body) error = %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("decompressed body = %q, want %q", data, payload)
	}
}

func TestJSONValueRequestBodyReportsMarshalErrors(t *testing.T) {
	body, contentEncoding, err := jsonValueRequestBody(map[string]interface{}{"bad": func() {}}, 0)
	if err == nil {
		t.Fatal("jsonValueRequestBody(unsupported) error = nil, want marshal error")
	}
	if body != nil {
		t.Fatalf("body = %T, want nil", body)
	}
	if contentEncoding != "" {
		t.Fatalf("Content-Encoding = %q, want empty", contentEncoding)
	}
}

func TestJSONValueRequestBodyStreamsLargeStructuredCommandPayload(t *testing.T) {
	values := make(hatriecache.Slice, 0, minCompressedJSONRequestBytes/4)
	for len(values) < cap(values) {
		values = append(values, strings.Repeat("value", 4))
	}
	payload := hatriecache.CacheCommandRequest{
		Command: "PUSHL",
		Key:     "jobs",
		Values:  values,
	}

	body, contentEncoding, err := jsonValueRequestBody(payload, estimatedCommandRequestBytes(payload))
	if err != nil {
		t.Fatalf("jsonValueRequestBody() error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	if _, ok := body.(*jsonwire.StreamingGzipJSONBody); !ok {
		t.Fatalf("jsonValueRequestBody() body = %T, want streaming gzip body", body)
	}

	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(streaming gzip body) error = %v", err)
	}
	defer reader.Close()

	var decoded hatriecache.CacheCommandRequest
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(streaming gzip body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded streaming payload = %#v, want %#v", decoded, payload)
	}
}

func TestEstimatedCommandRequestBytesUsesExactOptionalIntSize(t *testing.T) {
	priority := int64(3)
	ttl := int64(60)
	unix := int64(-7)
	base := hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "session:ttl", Value: "value"}
	withOptionals := base
	withOptionals.Priority = &priority
	withOptionals.TTLSeconds = &ttl
	withOptionals.UnixSeconds = &unix

	wantExtra := jsonwire.EstimateJSONValueBytes(priority, minCompressedJSONRequestBytes) +
		jsonwire.EstimateJSONValueBytes(ttl, minCompressedJSONRequestBytes) +
		jsonwire.EstimateJSONValueBytes(unix, minCompressedJSONRequestBytes)
	gotExtra := estimatedCommandRequestBytes(withOptionals) - estimatedCommandRequestBytes(base)
	if gotExtra != wantExtra {
		t.Fatalf("estimated optional int bytes = %d, want exact numeric bytes %d", gotExtra, wantExtra)
	}
	if got := addEstimatedOptionalCommandInt64(minCompressedJSONRequestBytes-1, &priority, minCompressedJSONRequestBytes); got != minCompressedJSONRequestBytes {
		t.Fatalf("addEstimatedOptionalCommandInt64(capped) = %d, want threshold", got)
	}
}

func TestCommandRequestBodyAutoFallsBackToJSONForComplexPayload(t *testing.T) {
	payload := hatriecache.CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs: hatriecache.Map{
			"nested": hatriecache.Map{"city": "Singapore"},
		},
	}
	body, contentType, contentEncoding, err := commandRequestBody(payload, "auto")
	if err != nil {
		t.Fatalf("commandRequestBody(auto complex) error = %v", err)
	}
	if contentType != "application/json" {
		t.Fatalf("contentType = %q, want application/json", contentType)
	}
	if contentEncoding != "" {
		t.Fatalf("contentEncoding = %q, want empty", contentEncoding)
	}
	var decoded hatriecache.CacheCommandRequest
	if err := json.NewDecoder(body).Decode(&decoded); err != nil {
		t.Fatalf("Decode(auto fallback JSON) error = %v", err)
	}
	if !reflect.DeepEqual(decoded.Pairs, payload.Pairs) {
		t.Fatalf("decoded pairs = %#v, want %#v", decoded.Pairs, payload.Pairs)
	}
}

func TestCommandRequestBodyForcedProtobufRejectsComplexPayload(t *testing.T) {
	payload := hatriecache.CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs: hatriecache.Map{
			"nested": hatriecache.Map{"city": "Singapore"},
		},
	}
	body, contentType, contentEncoding, err := commandRequestBody(payload, "protobuf")
	if err == nil {
		t.Fatal("commandRequestBody(protobuf complex) error = nil, want unsupported value error")
	}
	if body != nil || contentType != "" || contentEncoding != "" {
		t.Fatalf("commandRequestBody(protobuf complex) = %T/%q/%q, want nil/empty/empty", body, contentType, contentEncoding)
	}
}

func TestCommandRequestBodyRejectsUnsupportedWireFormat(t *testing.T) {
	body, contentType, contentEncoding, err := commandRequestBody(hatriecache.CacheCommandRequest{Command: "GET", Key: "key"}, "msgpack")
	if err == nil || !strings.Contains(err.Error(), "unsupported command wire format") {
		t.Fatalf("commandRequestBody(msgpack) error = %v, want unsupported format", err)
	}
	if body != nil || contentType != "" || contentEncoding != "" {
		t.Fatalf("commandRequestBody(msgpack) = %T/%q/%q, want nil/empty/empty", body, contentType, contentEncoding)
	}
}

func TestRunCommandPostsStructuredJSONFields(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		decoder.UseNumber()
		if err := decoder.Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"stored"}`))
	}))
	defer server.Close()

	unixSeconds := int64(1800)
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "PUTMAP",
		"-key", "profile",
		"-subkey", "city",
		"-value", "Singapore",
		"-pairs", `{"age":32}`,
		"-values", `["queued",7]`,
		"-priority", "3",
		"-unix-seconds", "1800",
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command structured) error = %v", err)
	}

	if gotRequest.Command != "PUTMAP" || gotRequest.Key != "profile" || gotRequest.Subkey != "city" || gotRequest.Value != "Singapore" {
		t.Fatalf("request basics = %#v, want PUTMAP profile city Singapore", gotRequest)
	}
	if gotRequest.UnixSeconds == nil || *gotRequest.UnixSeconds != unixSeconds {
		t.Fatalf("unix seconds = %v, want %d", gotRequest.UnixSeconds, unixSeconds)
	}
	if gotRequest.Priority == nil || *gotRequest.Priority != 3 {
		t.Fatalf("priority = %v, want 3", gotRequest.Priority)
	}
	if got := gotRequest.Pairs["age"]; got != json.Number("32") {
		t.Fatalf("pairs[age] = %#v, want json.Number(32)", got)
	}
	if len(gotRequest.Values) != 2 || gotRequest.Values[0] != "queued" || gotRequest.Values[1] != json.Number("7") {
		t.Fatalf("values = %#v, want queued and json.Number(7)", gotRequest.Values)
	}
}

func TestRunCommandPostsBatchFlag(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		decoder.UseNumber()
		if err := decoder.Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":false,"message":"batch completed with errors","responses":[{"ok":true,"message":"stored string"},{"ok":false,"message":"value must be a 32-bit integer"}]}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-batch", `[{"command":"SETSTR","key":"name","value":"ivi"},{"command":"SETINT","key":"bad","value":"not-int"}]`,
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command batch) error = %v", err)
	}

	if gotRequest.Command != "BATCH" || len(gotRequest.Batch) != 2 {
		t.Fatalf("batch request = %#v, want BATCH with two requests", gotRequest)
	}
	if gotRequest.Batch[0].Command != "SETSTR" || gotRequest.Batch[0].Key != "name" || gotRequest.Batch[0].Value != "ivi" {
		t.Fatalf("first batch request = %#v, want SETSTR name ivi", gotRequest.Batch[0])
	}
	if gotRequest.Batch[1].Command != "SETINT" || gotRequest.Batch[1].Value != "not-int" {
		t.Fatalf("second batch request = %#v, want SETINT bad value", gotRequest.Batch[1])
	}
}

func TestRunCommandPostsRadixTreeFields(t *testing.T) {
	var gotRequests []hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %q, want /api/commands", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		decoder := json.NewDecoder(r.Body)
		decoder.UseNumber()
		var request hatriecache.CacheCommandRequest
		if err := decoder.Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		gotRequests = append(gotRequests, request)
		w.Write([]byte(`{"ok":true,"message":"stored radix tree values"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "PUTRT",
		"-key", "index",
		"-subkey", "user:100/profile",
		"-value", "active",
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command PUTRT subkey) error = %v", err)
	}
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "PUTRT",
		"-key", "index",
		"-pairs", `{"user:101/profile":"idle","user:102/profile":42}`,
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command PUTRT pairs) error = %v", err)
	}

	if len(gotRequests) != 2 {
		t.Fatalf("requests = %d, want 2", len(gotRequests))
	}
	first := gotRequests[0]
	if first.Command != "PUTRT" || first.Key != "index" || first.Subkey != "user:100/profile" || first.Value != "active" {
		t.Fatalf("first request = %#v, want PUTRT index user:100/profile active", first)
	}
	second := gotRequests[1]
	if second.Command != "PUTRT" || second.Key != "index" {
		t.Fatalf("second request basics = %#v, want PUTRT index", second)
	}
	if got := second.Pairs["user:101/profile"]; got != "idle" {
		t.Fatalf("pairs[user:101/profile] = %#v, want idle", got)
	}
	if got := second.Pairs["user:102/profile"]; got != json.Number("42") {
		t.Fatalf("pairs[user:102/profile] = %#v, want json.Number(42)", got)
	}
}

func TestRunCommandRejectsInvalidStructuredFlagsBeforePost(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name: "priority",
			args: []string{
				"-addr", server.URL,
				"command",
				"-cmd", "PUSHPQ",
				"-key", "jobs",
				"-value", "job",
				"-priority", "not-int",
			},
			wantErr: "priority:",
		},
		{
			name: "values json",
			args: []string{
				"-addr", server.URL,
				"command",
				"-cmd", "PUSHSLICE",
				"-key", "jobs",
				"-values", `["build"`,
			},
			wantErr: "values:",
		},
		{
			name: "values trailing json",
			args: []string{
				"-addr", server.URL,
				"command",
				"-cmd", "PUSHSLICE",
				"-key", "jobs",
				"-values", `["build"] []`,
			},
			wantErr: "values: invalid trailing JSON",
		},
		{
			name: "pairs json",
			args: []string{
				"-addr", server.URL,
				"command",
				"-cmd", "PUTMAP",
				"-key", "profile",
				"-pairs", `{"age":`,
			},
			wantErr: "pairs:",
		},
		{
			name: "pairs trailing json",
			args: []string{
				"-addr", server.URL,
				"command",
				"-cmd", "PUTMAP",
				"-key", "profile",
				"-pairs", `{"age":32} {}`,
			},
			wantErr: "pairs: invalid trailing JSON",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := atomic.LoadInt32(&requests)
			err := run(context.Background(), tt.args, &bytes.Buffer{}, &bytes.Buffer{}, server.Client())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("run(command) error = %v, want %q", err, tt.wantErr)
			}
			if got := atomic.LoadInt32(&requests); got != before {
				t.Fatalf("requests = %d, want %d; invalid local flags should not post", got, before)
			}
		})
	}
}

func TestRunCommandPostsBloomFilterOptions(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"created bloom filter"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "CREATEBF",
		"-key", "seen",
		"-value", "10000",
		"-subkey", "0.001",
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command CREATEBF) error = %v", err)
	}

	if gotRequest.Command != "CREATEBF" || gotRequest.Key != "seen" || gotRequest.Value != "10000" || gotRequest.Subkey != "0.001" {
		t.Fatalf("request = %#v, want CREATEBF seen value 10000 subkey 0.001", gotRequest)
	}
}

func TestRunCommandPostsCuckooFilterOptions(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"created cuckoo filter"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "CREATECF",
		"-key", "active",
		"-value", "10000",
		"-subkey", "0.001",
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command CREATECF) error = %v", err)
	}

	if gotRequest.Command != "CREATECF" || gotRequest.Key != "active" || gotRequest.Value != "10000" || gotRequest.Subkey != "0.001" {
		t.Fatalf("request = %#v, want CREATECF active value 10000 subkey 0.001", gotRequest)
	}
}

func TestRunCommandPostsRoaringBitmapValues(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		decoder.UseNumber()
		if err := decoder.Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"added 2 roaring bitmap values"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "ADDRB",
		"-key", "ids",
		"-values", `[1,65543]`,
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command ADDRB) error = %v", err)
	}

	if gotRequest.Command != "ADDRB" || gotRequest.Key != "ids" {
		t.Fatalf("request = %#v, want ADDRB ids", gotRequest)
	}
	if len(gotRequest.Values) != 2 || gotRequest.Values[0] != json.Number("1") || gotRequest.Values[1] != json.Number("65543") {
		t.Fatalf("values = %#v, want json.Number values 1 and 65543", gotRequest.Values)
	}
}

func TestRunCommandPostsCountMinSketchOptions(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"created count-min sketch"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "CREATECMS",
		"-key", "freq",
		"-value", "2048",
		"-subkey", "4",
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command CREATECMS) error = %v", err)
	}

	if gotRequest.Command != "CREATECMS" || gotRequest.Key != "freq" || gotRequest.Value != "2048" || gotRequest.Subkey != "4" {
		t.Fatalf("request = %#v, want CREATECMS freq value 2048 subkey 4", gotRequest)
	}
}

func TestRunCommandPostsHyperLogLogOptions(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"created hyperloglog"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "CREATEHLL",
		"-key", "card",
		"-value", "14",
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command CREATEHLL) error = %v", err)
	}

	if gotRequest.Command != "CREATEHLL" || gotRequest.Key != "card" || gotRequest.Value != "14" {
		t.Fatalf("request = %#v, want CREATEHLL card value 14", gotRequest)
	}
}

func TestRunCommandPostsTopKOptions(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"created top-k"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "CREATETOPK",
		"-key", "top",
		"-value", "100",
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command CREATETOPK) error = %v", err)
	}

	if gotRequest.Command != "CREATETOPK" || gotRequest.Key != "top" || gotRequest.Value != "100" {
		t.Fatalf("request = %#v, want CREATETOPK top value 100", gotRequest)
	}
}

func TestRunCommandPostsReservoirSampleOptions(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"created reservoir sample"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "CREATERS",
		"-key", "sample",
		"-value", "128",
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command CREATERS) error = %v", err)
	}

	if gotRequest.Command != "CREATERS" || gotRequest.Key != "sample" || gotRequest.Value != "128" {
		t.Fatalf("request = %#v, want CREATERS sample value 128", gotRequest)
	}
}

func TestRunCommandPostsQuantileSketchOptions(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"created quantile sketch"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "CREATEQ",
		"-key", "latency",
		"-value", "0.02",
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command CREATEQ) error = %v", err)
	}

	if gotRequest.Command != "CREATEQ" || gotRequest.Key != "latency" || gotRequest.Value != "0.02" {
		t.Fatalf("request = %#v, want CREATEQ latency value 0.02", gotRequest)
	}
}

func TestRunCommandPostsFenwickTreeOptions(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"ok":true,"message":"updated fenwick tree"}`))
	}))
	defer server.Close()

	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"command",
		"-wire-format", "json",
		"-cmd", "ADDFW",
		"-key", "scores",
		"-value", "2",
		"-subkey", "5",
	}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(command ADDFW) error = %v", err)
	}

	if gotRequest.Command != "ADDFW" || gotRequest.Key != "scores" || gotRequest.Value != "2" || gotRequest.Subkey != "5" {
		t.Fatalf("request = %#v, want ADDFW scores value 2 subkey 5", gotRequest)
	}
}

func TestRunSnapshotPostsToSnapshotEndpoint(t *testing.T) {
	var gotPath string
	var gotMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		gotMethod = r.Method
		w.Write([]byte(`{"ok":true,"message":"snapshot saved"}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", server.URL, "snapshot"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(snapshot) error = %v", err)
	}
	if gotPath != "/api/snapshot" || gotMethod != http.MethodPost {
		t.Fatalf("request = %s %s, want POST /api/snapshot", gotMethod, gotPath)
	}
	if got := stdout.String(); got != "{\"ok\":true,\"message\":\"snapshot saved\"}\n" {
		t.Fatalf("stdout = %q, want snapshot response", got)
	}
}

func TestRunBackupPostsToBackupEndpoint(t *testing.T) {
	var gotPath string
	var gotMethod string
	var gotRequest struct {
		Path           string `json:"path"`
		Mode           string `json:"mode"`
		Retain         int    `json:"retain"`
		SnapshotFormat string `json:"snapshot_format"`
		Partition      struct {
			Mode                string   `json:"mode"`
			Partitions          []string `json:"partitions"`
			NodeID              string   `json:"node_id"`
			TopologyEpoch       uint64   `json:"topology_epoch"`
			TopologyFingerprint string   `json:"topology_fingerprint"`
			KeyPrefixes         []string `json:"key_prefixes"`
		} `json:"partition"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		gotMethod = r.Method
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		w.Write([]byte(`{"version":1,"snapshot":"snapshot.hc"}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"backup",
		"-path", "/tmp/backup.tar.gz",
		"-mode", "pebble-incremental",
		"-retain", "7",
		"-snapshot-format", "gzip-binary",
		"-partition-mode", "partitioned",
		"-partitions", "sg",
		"-partition-node", "node-sg-a",
		"-partition-epoch", "42",
		"-partition-fingerprint", "topology-v1",
		"-partition-prefixes", "sg:",
	}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(backup) error = %v", err)
	}
	if gotPath != "/api/backup" || gotMethod != http.MethodPost {
		t.Fatalf("request = %s %s, want POST /api/backup", gotMethod, gotPath)
	}
	if gotRequest.Path != "/tmp/backup.tar.gz" || gotRequest.Mode != "pebble-incremental" || gotRequest.Retain != 7 || gotRequest.SnapshotFormat != "gzip-binary" {
		t.Fatalf("backup request = %#v, want path and snapshot format", gotRequest)
	}
	if gotRequest.Partition.Mode != "partitioned" || !reflect.DeepEqual(gotRequest.Partition.Partitions, []string{"sg"}) || gotRequest.Partition.NodeID != "node-sg-a" || gotRequest.Partition.TopologyEpoch != 42 || gotRequest.Partition.TopologyFingerprint != "topology-v1" || !reflect.DeepEqual(gotRequest.Partition.KeyPrefixes, []string{"sg:"}) {
		t.Fatalf("backup partition request = %#v, want requested partition metadata", gotRequest.Partition)
	}
	if got := stdout.String(); got != "{\"version\":1,\"snapshot\":\"snapshot.hc\"}\n" {
		t.Fatalf("stdout = %q, want backup response", got)
	}
}

func TestRunBackupRequiresPath(t *testing.T) {
	err := run(context.Background(), []string{"backup"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "backup -path is required") {
		t.Fatalf("run(backup without path) error = %v, want path requirement", err)
	}
}

func TestRunBackupAndVerifyRunsAllServerStages(t *testing.T) {
	var sequence []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sequence = append(sequence, r.URL.Path)
		var request struct {
			Path string `json:"path"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode(%s) error = %v", r.URL.Path, err)
		}
		if request.Path != "/srv/backups/run-001.tar.gz" {
			t.Fatalf("%s path = %q", r.URL.Path, request.Path)
		}
		switch r.URL.Path {
		case "/api/backup":
			json.NewEncoder(w).Encode(hatriecache.BackupBundleManifest{Version: 1, Mode: hatriecache.BackupModeSnapshot, Snapshot: "snapshot.hc"})
		case "/api/backup/verify":
			json.NewEncoder(w).Encode(hatriecache.BackupDoctorReport{OK: true, Kind: "bundle", RecoveredKeys: 7})
		case "/api/backup/rehearse":
			json.NewEncoder(w).Encode(hatriecache.RestoreRehearsalReport{OK: true, SourceKind: "bundle", RecoveredKeys: 7})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"backup-and-verify",
		"-path", "/srv/backups/run-001.tar.gz",
		"-mode", "snapshot",
	}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(backup-and-verify) error = %v", err)
	}
	if !reflect.DeepEqual(sequence, []string{"/api/backup", "/api/backup/verify", "/api/backup/rehearse"}) {
		t.Fatalf("backup-and-verify sequence = %#v", sequence)
	}
	var result backupAndVerifyResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(backup-and-verify result) error = %v", err)
	}
	if !result.OK || result.Manifest.Version != 1 || !result.Verification.OK || result.Rehearsal == nil || !result.Rehearsal.OK || result.Rehearsal.RecoveredKeys != 7 {
		t.Fatalf("backup-and-verify result = %#v", result)
	}
	if result.DurationMillis < 0 || result.CreateMillis < 0 || result.VerifyMillis < 0 || result.RehearseMillis < 0 {
		t.Fatalf("backup stage durations = %#v", result)
	}
}

func TestRunSupportBundleCollectsAllNodesAndRedactsSecrets(t *testing.T) {
	var nodeAURL string
	var nodeBURL string
	topology := func(self string) hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeFullReplica,
			Self:    self,
			Nodes: []hatriecache.TopologyNode{
				{ID: "node-a", Address: nodeAURL, Role: "primary"},
				{ID: "node-b", Address: nodeBURL, Role: "replica"},
			},
		}
	}
	serveNode := func(nodeID string, writer http.ResponseWriter, request *http.Request) bool {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/health":
			json.NewEncoder(writer).Encode(hatriecache.MonitoringHealth{Status: "online", Node: nodeID, Version: "v1.2.3"})
		case "/api/config":
			json.NewEncoder(writer).Encode(map[string]interface{}{
				"node_id":               nodeID,
				"monitoring_auth_token": "raw-operator-secret",
				"nested":                map[string]interface{}{"password": "raw-password"},
			})
		case "/api/topology":
			json.NewEncoder(writer).Encode(topology(nodeID))
		case "/api/election":
			json.NewEncoder(writer).Encode(hatriecache.ElectionStatus{})
		case "/api/replication":
			json.NewEncoder(writer).Encode(hatriecache.ReplicationResult{Health: "healthy"})
		case "/api/storage":
			writer.Write([]byte(`{"configured":true,"store":"pebble"}`))
		case "/api/audit":
			json.NewEncoder(writer).Encode(map[string]interface{}{"configured": true, "events": []interface{}{}})
		case "/metrics":
			writer.Header().Set("Content-Type", "text/plain")
			writer.Write([]byte("hatrie_cache_up 1\n"))
		default:
			return false
		}
		return true
	}
	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !serveNode("node-b", w, r) {
			t.Fatalf("unexpected node-b request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer nodeB.Close()
	nodeBURL = nodeB.URL
	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !serveNode("node-a", w, r) {
			t.Fatalf("unexpected node-a request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer nodeA.Close()
	nodeAURL = nodeA.URL

	bundlePath := filepath.Join(t.TempDir(), "support.tar.gz")
	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", nodeA.URL,
		"support-bundle",
		"-path", bundlePath,
	}, stdout, &bytes.Buffer{}, nodeA.Client()); err != nil {
		t.Fatalf("run(support-bundle) error = %v", err)
	}
	info, err := os.Stat(bundlePath)
	if err != nil {
		t.Fatalf("Stat(support bundle) error = %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("support bundle mode = %o, want 600", info.Mode().Perm())
	}
	file, err := os.Open(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gzipReader.Close()
	entries := map[string][]byte{}
	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(tarReader)
		if err != nil {
			t.Fatal(err)
		}
		entries[header.Name] = data
	}
	for _, name := range []string{
		"manifest.json",
		"nodes/001-node-a/config.json",
		"nodes/001-node-a/metrics.txt",
		"nodes/002-node-b/health.json",
		"nodes/002-node-b/topology.json",
	} {
		if _, ok := entries[name]; !ok {
			t.Fatalf("support bundle missing %s; entries = %#v", name, reflect.ValueOf(entries).MapKeys())
		}
	}
	allData := bytes.Join(func() [][]byte {
		out := make([][]byte, 0, len(entries))
		for _, data := range entries {
			out = append(out, data)
		}
		return out
	}(), nil)
	if bytes.Contains(allData, []byte("raw-operator-secret")) || bytes.Contains(allData, []byte("raw-password")) {
		t.Fatalf("support bundle contains unredacted secret: %s", allData)
	}
	if !bytes.Contains(entries["nodes/001-node-a/config.json"], []byte("[REDACTED]")) {
		t.Fatalf("redacted config = %s", entries["nodes/001-node-a/config.json"])
	}
	var result supportBundleResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(support bundle result) error = %v", err)
	}
	if !result.OK || result.Nodes != 2 || result.Bytes <= 0 || len(result.SHA256) != 64 {
		t.Fatalf("support bundle result = %#v", result)
	}
}

func TestRunSupportBundleRecordsEndpointFailuresAndContinues(t *testing.T) {
	var serverURL string
	topology := hatriecache.ClusterTopology{
		Version: 1,
		Mode:    hatriecache.TopologyModeFullReplica,
		Self:    "node-a",
		Nodes:   []hatriecache.TopologyNode{{ID: "node-a", Address: serverURL, Role: "primary"}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/topology":
			topology.Nodes[0].Address = serverURL
			json.NewEncoder(w).Encode(topology)
		case "/api/replication":
			http.Error(w, "replication unavailable", http.StatusServiceUnavailable)
		case "/metrics":
			w.Write([]byte("hatrie_cache_up 1\n"))
		default:
			json.NewEncoder(w).Encode(map[string]interface{}{"node": "node-a"})
		}
	}))
	defer server.Close()
	serverURL = server.URL

	bundlePath := filepath.Join(t.TempDir(), "support.tar.gz")
	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", server.URL,
		"support-bundle",
		"-path", bundlePath,
	}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(support-bundle) error = %v", err)
	}
	var result supportBundleResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(support bundle result) error = %v", err)
	}
	if result.OK || result.Errors != 1 {
		t.Fatalf("support bundle result = %#v, want one recorded error", result)
	}

	file, err := os.Open(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)
	var manifest supportBundleManifest
	seenMetrics := false
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(tarReader)
		if err != nil {
			t.Fatal(err)
		}
		switch header.Name {
		case "manifest.json":
			if err := json.Unmarshal(data, &manifest); err != nil {
				t.Fatal(err)
			}
		case "nodes/001-node-a/metrics.txt":
			seenMetrics = true
		}
	}
	if !seenMetrics || len(manifest.Nodes) != 1 || !strings.Contains(manifest.Nodes[0].Errors["replication.json"], "503") {
		t.Fatalf("support bundle manifest = %#v, seen metrics = %t", manifest, seenMetrics)
	}
}

func TestRunDoctorVerifiesBackupPath(t *testing.T) {
	dir := t.TempDir()
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	if got := ht.ExecuteCommand(hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("SETSTR response = %#v, want ok", got)
	}
	if err := ht.SaveSnapshotWithFormat(filepath.Join(dir, "snapshot.hc"), hatriecache.SnapshotFormatJSON); err != nil {
		t.Fatalf("SaveSnapshotWithFormat() error = %v", err)
	}

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"doctor", "-path", dir}, stdout, &bytes.Buffer{}, http.DefaultClient); err != nil {
		t.Fatalf("run(doctor) error = %v", err)
	}
	var report hatriecache.BackupDoctorReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("Unmarshal(doctor report) error = %v", err)
	}
	if !report.OK || report.Kind != "directory" || report.RecoveredKeys != 1 {
		t.Fatalf("doctor report = %#v, want ok directory with one key", report)
	}
}

func TestRunDoctorRequiresPath(t *testing.T) {
	err := run(context.Background(), []string{"doctor"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "doctor -path is required") {
		t.Fatalf("run(doctor without path) error = %v, want path requirement", err)
	}
}

func TestRunRestoreBundleVerifiesAndRestores(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	if got := ht.ExecuteCommand(hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("SETSTR response = %#v, want ok", got)
	}
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := hatriecache.CreateBackupBundle(bundlePath, ht, nil, hatriecache.BackupBundleOptions{SnapshotFormat: hatriecache.SnapshotFormatJSON}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	dataDir := filepath.Join(t.TempDir(), "data")
	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"restore-bundle", "-bundle", bundlePath, "-data-dir", dataDir}, stdout, &bytes.Buffer{}, http.DefaultClient); err != nil {
		t.Fatalf("run(restore-bundle) error = %v", err)
	}
	var report hatriecache.BackupBundleRestoreReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("Unmarshal(restore report) error = %v", err)
	}
	if !report.OK || report.RecoveredKeys != 1 || report.Snapshot == "" {
		t.Fatalf("restore report = %#v, want ok with snapshot", report)
	}
}

func TestRunRestoreBundleRequiresBundle(t *testing.T) {
	err := run(context.Background(), []string{"restore-bundle"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "restore-bundle -bundle is required") {
		t.Fatalf("run(restore-bundle without bundle) error = %v, want bundle requirement", err)
	}
}

func TestRunRestoreRehearsalVerifiesBackupPath(t *testing.T) {
	dir := t.TempDir()
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	if got := ht.ExecuteCommand(hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("SETSTR response = %#v, want ok", got)
	}
	if err := ht.SaveSnapshotWithFormat(filepath.Join(dir, "snapshot.hc"), hatriecache.SnapshotFormatJSON); err != nil {
		t.Fatalf("SaveSnapshotWithFormat() error = %v", err)
	}

	stdout := &bytes.Buffer{}
	workDir := filepath.Join(t.TempDir(), "rehearsal")
	if err := run(context.Background(), []string{"restore-rehearsal", "-path", dir, "-work-dir", workDir, "-runtime-get", "name=ivi"}, stdout, &bytes.Buffer{}, http.DefaultClient); err != nil {
		t.Fatalf("run(restore-rehearsal) error = %v", err)
	}
	var report hatriecache.RestoreRehearsalReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("Unmarshal(rehearsal report) error = %v", err)
	}
	if !report.OK || report.SourceKind != "directory" || report.RecoveredKeys != 1 || report.RestoredDir == "" {
		t.Fatalf("rehearsal report = %#v, want ok directory with one key", report)
	}
	if report.Runtime == nil || !report.Runtime.OK || report.Runtime.Health == nil || report.Runtime.Stats == nil || len(report.Runtime.Gets) != 1 {
		t.Fatalf("runtime report = %#v, want health/stats/one GET", report.Runtime)
	}
	if got := report.Runtime.Gets[0]; !got.OK || got.Key != "name" || got.Value != "ivi" || got.Expected == nil || *got.Expected != "ivi" {
		t.Fatalf("runtime GET = %#v, want name=ivi", got)
	}
}

func TestRunRestoreRehearsalRequiresPath(t *testing.T) {
	err := run(context.Background(), []string{"restore-rehearsal"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "restore-rehearsal -path is required") {
		t.Fatalf("run(restore-rehearsal without path) error = %v, want path requirement", err)
	}
}

func TestClusterJoinTopologyAddsReplica(t *testing.T) {
	topology := hatriecache.ClusterTopology{
		Version: 1,
		Mode:    hatriecache.TopologyModeSharded,
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080", Role: "primary"},
		},
		Shards: []hatriecache.TopologyShard{
			{ID: 0, Primary: "node-a"},
		},
	}
	updated, changed, err := clusterJoinTopology(topology, "node-b", "http://node-b:8080", "replica")
	if err != nil {
		t.Fatalf("clusterJoinTopology() error = %v", err)
	}
	if !changed {
		t.Fatal("clusterJoinTopology() changed = false, want true")
	}
	if len(updated.Nodes) != 2 || updated.Nodes[1].ID != "node-b" {
		t.Fatalf("nodes = %#v, want node-b added", updated.Nodes)
	}
	if len(updated.Shards) != 1 || !reflect.DeepEqual(updated.Shards[0].Replicas, []string{"node-b"}) {
		t.Fatalf("shards = %#v, want node-b replica", updated.Shards)
	}

	again, changed, err := clusterJoinTopology(updated, "node-b", "http://node-b:8080", "replica")
	if err != nil {
		t.Fatalf("clusterJoinTopology(existing) error = %v", err)
	}
	if changed || !reflect.DeepEqual(again, updated) {
		t.Fatalf("existing join changed topology = %v %#v, want unchanged", changed, again)
	}
	if _, _, err := clusterJoinTopology(updated, "node-b", "http://node-b-new:8080", "replica"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("clusterJoinTopology(identity collision) error = %v, want explicit replacement requirement", err)
	}
}

func TestClusterAddReplicaTopologyRejectsIdentityConflicts(t *testing.T) {
	topology := hatriecache.ClusterTopology{
		Version: 1,
		Mode:    hatriecache.TopologyModeFullReplica,
		Self:    "node-a",
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080", Role: "primary"},
			{ID: "node-b", Address: "http://node-b-old:8080", Role: "replica"},
			{ID: "node-c", Address: "http://node-c:8080", Role: "replica"},
		},
	}

	if _, _, _, err := clusterAddReplicaTopology(topology, "node-b", "http://node-b-new:8080", false); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("clusterAddReplicaTopology(id collision) error = %v, want existing-id rejection", err)
	}
	updated, changed, replaced, err := clusterAddReplicaTopology(topology, "node-b", "http://node-b-new:8080", true)
	if err != nil {
		t.Fatalf("clusterAddReplicaTopology(replace) error = %v", err)
	}
	if !changed || !replaced {
		t.Fatalf("clusterAddReplicaTopology(replace) changed/replaced = %v/%v, want true/true", changed, replaced)
	}
	for _, node := range updated.Nodes {
		if node.ID == "node-b" && node.Address != "http://node-b-new:8080" {
			t.Fatalf("replacement node address = %q, want new address", node.Address)
		}
	}

	if _, _, _, err := clusterAddReplicaTopology(topology, "node-d", "http://node-c:8080", true); err == nil || !strings.Contains(err.Error(), "already belongs") {
		t.Fatalf("clusterAddReplicaTopology(address collision) error = %v, want address-owner rejection", err)
	}
	if _, _, _, err := clusterAddReplicaTopology(topology, "node-a", "http://node-a:8080", true); err == nil || !strings.Contains(err.Error(), "primary") {
		t.Fatalf("clusterAddReplicaTopology(primary) error = %v, want primary rejection", err)
	}
}

func TestClusterAddReplicaTopologyIsIdempotent(t *testing.T) {
	topology := hatriecache.ClusterTopology{
		Version: 1,
		Mode:    hatriecache.TopologyModeSharded,
		Self:    "node-a",
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080", Role: "primary"},
			{ID: "node-b", Address: "http://node-b:8080", Role: "replica"},
		},
		Shards: []hatriecache.TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	}
	updated, changed, replaced, err := clusterAddReplicaTopology(topology, "node-b", "http://node-b:8080/", false)
	if err != nil {
		t.Fatalf("clusterAddReplicaTopology(idempotent) error = %v", err)
	}
	if changed || replaced {
		t.Fatalf("clusterAddReplicaTopology(idempotent) changed/replaced = %v/%v, want false/false", changed, replaced)
	}
	if !reflect.DeepEqual(updated, topology) {
		t.Fatalf("clusterAddReplicaTopology(idempotent) = %#v, want %#v", updated, topology)
	}
}

func TestClusterRemoveTopologyRemovesReplica(t *testing.T) {
	topology := hatriecache.ClusterTopology{
		Version: 1,
		Mode:    hatriecache.TopologyModeSharded,
		Self:    "node-a",
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080", Role: "primary"},
			{ID: "node-b", Address: "http://node-b:8080", Role: "replica"},
			{ID: "node-c", Address: "http://node-c:8080", Role: "replica"},
		},
		Shards: []hatriecache.TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b", "node-c"}},
		},
	}

	updated, changed, err := clusterRemoveTopology(topology, "node-b")
	if err != nil {
		t.Fatalf("clusterRemoveTopology() error = %v", err)
	}
	if !changed {
		t.Fatal("clusterRemoveTopology() changed = false, want true")
	}
	if len(updated.Nodes) != 2 || updated.Nodes[0].ID != "node-a" || updated.Nodes[1].ID != "node-c" {
		t.Fatalf("nodes = %#v, want node-a and node-c", updated.Nodes)
	}
	if len(updated.Shards) != 1 || !reflect.DeepEqual(updated.Shards[0].Replicas, []string{"node-c"}) {
		t.Fatalf("shards = %#v, want only node-c replica", updated.Shards)
	}

	again, changed, err := clusterRemoveTopology(updated, "node-b")
	if err != nil {
		t.Fatalf("clusterRemoveTopology(missing) error = %v", err)
	}
	if changed || !reflect.DeepEqual(again, updated) {
		t.Fatalf("idempotent remove changed topology = %v %#v, want unchanged", changed, again)
	}

	if _, _, err := clusterRemoveTopology(topology, "node-a"); err == nil || !strings.Contains(err.Error(), "primary") {
		t.Fatalf("clusterRemoveTopology(primary) error = %v, want primary refusal", err)
	}
}

func TestValidateClusterDecommissionRequiresRemainingReplica(t *testing.T) {
	topology := hatriecache.ClusterTopology{
		Version: 1,
		Mode:    hatriecache.TopologyModeSharded,
		Self:    "node-a",
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080", Role: "primary"},
			{ID: "node-b", Address: "http://node-b:8080", Role: "replica"},
		},
		Shards: []hatriecache.TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	}
	if _, err := validateClusterDecommission(topology, "node-b", 1); err == nil || !strings.Contains(err.Error(), "remaining replicas") {
		t.Fatalf("validateClusterDecommission(last replica) error = %v, want redundancy refusal", err)
	}
	node, err := validateClusterDecommission(topology, "node-b", 0)
	if err != nil {
		t.Fatalf("validateClusterDecommission(min 0) error = %v", err)
	}
	if node.ID != "node-b" {
		t.Fatalf("validateClusterDecommission() node = %#v, want node-b", node)
	}
	if _, err := validateClusterDecommission(topology, "node-a", 0); err == nil || !strings.Contains(err.Error(), "primary") {
		t.Fatalf("validateClusterDecommission(primary) error = %v, want primary refusal", err)
	}
}

func TestTopologyForNodeSetsLocalIdentity(t *testing.T) {
	topology := hatriecache.ClusterTopology{
		Version: 1,
		Mode:    hatriecache.TopologyModeFullReplica,
		Self:    "node-a",
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080", Role: "primary"},
			{ID: "node-b", Address: "http://node-b:8080", Role: "replica"},
		},
	}
	updated := topologyForNode(topology, "node-b")
	if updated.Self != "node-b" {
		t.Fatalf("topologyForNode() self = %q, want node-b", updated.Self)
	}
	if topology.Self != "node-a" {
		t.Fatalf("topologyForNode() mutated input self = %q, want node-a", topology.Self)
	}
}

func TestRunClusterStatusReportsPeerAndNodeHealth(t *testing.T) {
	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-a"})
		case "/api/topology":
			json.NewEncoder(w).Encode(hatriecache.ClusterTopology{
				Version: 1,
				Mode:    hatriecache.TopologyModeFullReplica,
				Self:    "node-a",
				Nodes: []hatriecache.TopologyNode{
					{ID: "node-a", Address: serverURL, Role: "primary"},
				},
			})
		case "/api/election":
			json.NewEncoder(w).Encode(hatriecache.ElectionStatus{
				Nodes: []hatriecache.ElectionNodeStatus{{ID: "node-a", Online: true}},
			})
		case "/api/replication":
			json.NewEncoder(w).Encode(hatriecache.ReplicationResult{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", server.URL, "cluster", "status"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(cluster status) error = %v", err)
	}
	var result clusterStatusResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster status) error = %v", err)
	}
	if !result.OK || result.Health == nil || result.Topology == nil || result.Election == nil || len(result.Nodes) != 1 || !result.Nodes[0].OK {
		t.Fatalf("cluster status result = %#v, want healthy peer and node", result)
	}
	if !result.Nodes[0].TopologyConsistent || !result.Nodes[0].ElectionOK {
		t.Fatalf("cluster node status = %#v, want topology and election probes healthy", result.Nodes[0])
	}
}

func TestRunClusterStatusReportsTopologyDrift(t *testing.T) {
	var nodeAURL string
	var nodeBURL string
	referenceTopology := func() hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeFullReplica,
			Nodes: []hatriecache.TopologyNode{
				{ID: "node-a", Address: nodeAURL, Role: "primary"},
				{ID: "node-b", Address: nodeBURL, Role: "replica"},
			},
		}
	}
	driftedTopology := func() hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeFullReplica,
			Nodes: []hatriecache.TopologyNode{
				{ID: "node-b", Address: nodeBURL, Role: "replica"},
			},
		}
	}
	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-b"})
		case "/api/topology":
			json.NewEncoder(w).Encode(driftedTopology())
		case "/api/election":
			json.NewEncoder(w).Encode(hatriecache.ElectionStatus{Nodes: []hatriecache.ElectionNodeStatus{{ID: "node-b", Online: true}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()
	nodeBURL = nodeB.URL

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-a"})
		case "/api/topology":
			json.NewEncoder(w).Encode(referenceTopology())
		case "/api/election":
			json.NewEncoder(w).Encode(hatriecache.ElectionStatus{Nodes: []hatriecache.ElectionNodeStatus{{ID: "node-a", Online: true}}})
		case "/api/replication":
			json.NewEncoder(w).Encode(hatriecache.ReplicationResult{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeA.Close()
	nodeAURL = nodeA.URL

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", nodeA.URL, "cluster", "doctor"}, stdout, &bytes.Buffer{}, nodeA.Client()); err != nil {
		t.Fatalf("run(cluster doctor) error = %v", err)
	}
	var result clusterStatusResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster doctor) error = %v", err)
	}
	if result.OK {
		t.Fatalf("cluster doctor result = %#v, want unhealthy due topology drift", result)
	}
	var nodeBStatus *clusterNodeStatus
	for idx := range result.Nodes {
		if result.Nodes[idx].ID == "node-b" {
			nodeBStatus = &result.Nodes[idx]
			break
		}
	}
	if nodeBStatus == nil || nodeBStatus.OK || nodeBStatus.TopologyConsistent || !strings.Contains(nodeBStatus.TopologyError, "differs") {
		t.Fatalf("node-b status = %#v, want topology drift", nodeBStatus)
	}
	if len(result.Errors) == 0 || !strings.Contains(strings.Join(result.Errors, "\n"), "topology differs") {
		t.Fatalf("cluster doctor errors = %#v, want topology drift error", result.Errors)
	}
}

func TestRunClusterDoctorRepairsTopologyFromConfirmedPeer(t *testing.T) {
	var nodeAURL string
	var nodeBURL string
	reference := func(self string) hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeFullReplica,
			Self:    self,
			Nodes: []hatriecache.TopologyNode{
				{ID: "node-a", Address: nodeAURL, Role: "primary"},
				{ID: "node-b", Address: nodeBURL, Role: "replica"},
			},
		}
	}
	drifted := func() hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeFullReplica,
			Self:    "node-b",
			Nodes:   []hatriecache.TopologyNode{{ID: "node-b", Address: nodeBURL, Role: "replica"}},
		}
	}
	var nodeATopology hatriecache.ClusterTopology
	var nodeBTopology hatriecache.ClusterTopology
	var nodeAPuts int
	var nodeBPuts int
	election := hatriecache.ElectionStatus{Nodes: []hatriecache.ElectionNodeStatus{
		{ID: "node-a", Online: true}, {ID: "node-b", Online: true},
	}}

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "GET /api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-b"})
		case "GET /api/topology":
			if len(nodeBTopology.Nodes) == 0 {
				json.NewEncoder(w).Encode(drifted())
			} else {
				json.NewEncoder(w).Encode(nodeBTopology)
			}
		case "PUT /api/topology":
			nodeBPuts++
			json.NewDecoder(r.Body).Decode(&nodeBTopology)
			json.NewEncoder(w).Encode(nodeBTopology)
		case "GET /api/election":
			json.NewEncoder(w).Encode(election)
		default:
			t.Fatalf("unexpected node-b request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer nodeB.Close()
	nodeBURL = nodeB.URL

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "GET /api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-a"})
		case "GET /api/topology":
			if len(nodeATopology.Nodes) == 0 {
				json.NewEncoder(w).Encode(reference("node-a"))
			} else {
				json.NewEncoder(w).Encode(nodeATopology)
			}
		case "PUT /api/topology":
			nodeAPuts++
			json.NewDecoder(r.Body).Decode(&nodeATopology)
			json.NewEncoder(w).Encode(nodeATopology)
		case "GET /api/election":
			json.NewEncoder(w).Encode(election)
		case "GET /api/replication":
			json.NewEncoder(w).Encode(hatriecache.ReplicationResult{Health: "healthy", HealthScore: 100})
		default:
			t.Fatalf("unexpected node-a request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer nodeA.Close()
	nodeAURL = nodeA.URL

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", nodeA.URL,
		"cluster", "doctor",
		"-repair-topology",
		"-yes",
	}, stdout, &bytes.Buffer{}, nodeA.Client()); err != nil {
		t.Fatalf("run(cluster doctor repair) error = %v", err)
	}
	if nodeAPuts != 1 || nodeBPuts != 1 {
		t.Fatalf("topology puts = node-a %d node-b %d, want one each", nodeAPuts, nodeBPuts)
	}
	if nodeATopology.Self != "node-a" || nodeBTopology.Self != "node-b" {
		t.Fatalf("repaired self values = %q/%q", nodeATopology.Self, nodeBTopology.Self)
	}
	var result clusterStatusResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster doctor repair result) error = %v", err)
	}
	if !result.OK || result.TopologyRepair == nil || !result.TopologyRepair.Applied || !reflect.DeepEqual(result.TopologyRepair.NodesUpdated, []string{"node-a", "node-b"}) {
		t.Fatalf("cluster doctor repair result = %#v, want verified repair", result)
	}
}

func TestRunClusterDoctorRepairRequiresConfirmation(t *testing.T) {
	err := run(context.Background(), []string{"cluster", "doctor", "-repair-topology"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "-yes") {
		t.Fatalf("run(cluster doctor repair without confirmation) error = %v, want -yes requirement", err)
	}
}

func TestRunClusterStatusReportsElectionDrift(t *testing.T) {
	var nodeAURL string
	var nodeBURL string
	topology := func() hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeFullReplica,
			Nodes: []hatriecache.TopologyNode{
				{ID: "node-a", Address: nodeAURL, Role: "primary"},
				{ID: "node-b", Address: nodeBURL, Role: "replica"},
			},
		}
	}
	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-b"})
		case "/api/topology":
			json.NewEncoder(w).Encode(topology())
		case "/api/election":
			json.NewEncoder(w).Encode(hatriecache.ElectionStatus{
				Leaders: []hatriecache.ElectionLeader{{Shard: 0, Leader: "node-b", Available: true}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()
	nodeBURL = nodeB.URL

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-a"})
		case "/api/topology":
			json.NewEncoder(w).Encode(topology())
		case "/api/election":
			json.NewEncoder(w).Encode(hatriecache.ElectionStatus{
				Leaders: []hatriecache.ElectionLeader{{Shard: 0, Leader: "node-a", Available: true}},
			})
		case "/api/replication":
			json.NewEncoder(w).Encode(hatriecache.ReplicationResult{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeA.Close()
	nodeAURL = nodeA.URL

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", nodeA.URL, "cluster", "doctor"}, stdout, &bytes.Buffer{}, nodeA.Client()); err != nil {
		t.Fatalf("run(cluster doctor) error = %v", err)
	}
	var result clusterStatusResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster doctor) error = %v", err)
	}
	if result.OK {
		t.Fatalf("cluster doctor result = %#v, want unhealthy due election drift", result)
	}
	var nodeBStatus *clusterNodeStatus
	for idx := range result.Nodes {
		if result.Nodes[idx].ID == "node-b" {
			nodeBStatus = &result.Nodes[idx]
			break
		}
	}
	if nodeBStatus == nil || nodeBStatus.OK || !nodeBStatus.TopologyConsistent || nodeBStatus.ElectionConsistent || !strings.Contains(nodeBStatus.ElectionError, "leaders differ") {
		t.Fatalf("node-b status = %#v, want election drift only", nodeBStatus)
	}
}

func TestRunClusterDoctorAliasSkipsNodeProbe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-a"})
		case "/api/topology":
			json.NewEncoder(w).Encode(hatriecache.ClusterTopology{Version: 1, Mode: hatriecache.TopologyModeFullReplica})
		case "/api/election":
			json.NewEncoder(w).Encode(hatriecache.ElectionStatus{})
		case "/api/replication":
			json.NewEncoder(w).Encode(hatriecache.ReplicationResult{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"cluster", "doctor", "-peer", server.URL, "-probe-nodes=false"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(cluster doctor) error = %v", err)
	}
	var result clusterStatusResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster doctor) error = %v", err)
	}
	if !result.OK || result.Peer != server.URL || len(result.Nodes) != 0 {
		t.Fatalf("cluster doctor result = %#v, want healthy peer without node probes", result)
	}
}

func TestRunClusterConfigDiffIgnoresNodeLocalFieldsAndReportsDrift(t *testing.T) {
	var nodeAURL string
	var nodeBURL string
	topology := func() hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeFullReplica,
			Self:    "node-a",
			Nodes: []hatriecache.TopologyNode{
				{ID: "node-a", Address: nodeAURL, Role: "primary"},
				{ID: "node-b", Address: nodeBURL, Role: "replica"},
			},
		}
	}
	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/config" {
			t.Fatalf("unexpected node-b request %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"node_id":               "node-b",
			"db_path":               "/srv/node-b/cache",
			"db_format":             "json",
			"replication_mode":      "dual",
			"monitoring_auth_token": "configured",
		})
	}))
	defer nodeB.Close()
	nodeBURL = nodeB.URL

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/topology":
			json.NewEncoder(w).Encode(topology())
		case "/api/config":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"node_id":               "node-a",
				"db_path":               "/srv/node-a/cache",
				"db_format":             "binary",
				"replication_mode":      "dual",
				"monitoring_auth_token": "configured",
			})
		default:
			t.Fatalf("unexpected node-a request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer nodeA.Close()
	nodeAURL = nodeA.URL

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", nodeA.URL,
		"cluster", "config-diff",
	}, stdout, &bytes.Buffer{}, nodeA.Client()); err != nil {
		t.Fatalf("run(cluster config-diff) error = %v", err)
	}
	var result clusterConfigDiffResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster config-diff result) error = %v", err)
	}
	if result.OK || result.ReferenceNode != "node-a" || len(result.Nodes) != 2 {
		t.Fatalf("config diff result = %#v, want one drift from node-a", result)
	}
	var nodeBResult *clusterConfigNodeDiff
	for idx := range result.Nodes {
		if result.Nodes[idx].ID == "node-b" {
			nodeBResult = &result.Nodes[idx]
		}
	}
	if nodeBResult == nil || nodeBResult.OK || len(nodeBResult.Differences) != 1 || nodeBResult.Differences[0].Field != "db_format" {
		t.Fatalf("node-b config diff = %#v, want only db_format", nodeBResult)
	}
	if len(result.IgnoredFields) == 0 || !stringInSlice(result.IgnoredFields, "node_id") || !stringInSlice(result.IgnoredFields, "db_path") {
		t.Fatalf("ignored fields = %#v, want node-local fields", result.IgnoredFields)
	}
}

func TestRunClusterConfigDiffCanIncludeNodeLocalFields(t *testing.T) {
	differences := diffClusterConfigs(
		map[string]interface{}{"node_id": "node-a", "db_format": "binary"},
		map[string]interface{}{"node_id": "node-b", "db_format": "binary"},
		nil,
	)
	if len(differences) != 1 || differences[0].Field != "node_id" {
		t.Fatalf("diffClusterConfigs(include local) = %#v, want node_id", differences)
	}
}

func TestRunClusterUpgradePlanOrdersReplicasAndValidatesReadiness(t *testing.T) {
	var nodeAURL string
	var nodeBURL string
	backupTime := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	topology := func() hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeSharded,
			Self:    "node-a",
			Nodes: []hatriecache.TopologyNode{
				{ID: "node-a", Address: nodeAURL, Role: "primary"},
				{ID: "node-b", Address: nodeBURL, Role: "replica"},
			},
			Shards: []hatriecache.TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
		}
	}
	serveNodeState := func(nodeID string, writer http.ResponseWriter, request *http.Request) bool {
		switch request.URL.Path {
		case "/api/health":
			json.NewEncoder(writer).Encode(hatriecache.MonitoringHealth{Status: "online", Node: nodeID, APIVersion: hatriecache.MonitoringAPIVersion, Version: "v1.2.3"})
		case "/api/storage":
			writer.Write([]byte(`{"configured":true,"store":"pebble"}`))
		case "/api/config":
			json.NewEncoder(writer).Encode(map[string]interface{}{
				"node_id":          nodeID,
				"db_path":          "/srv/" + nodeID,
				"db_format":        "binary",
				"replication_mode": "dual",
			})
		case "/api/audit":
			json.NewEncoder(writer).Encode(map[string]interface{}{
				"configured": true,
				"events":     []hatriecache.AuditEvent{{Time: backupTime, Action: "backup", OK: true}},
			})
		default:
			return false
		}
		return true
	}
	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !serveNodeState("node-b", w, r) {
			t.Fatalf("unexpected node-b request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer nodeB.Close()
	nodeBURL = nodeB.URL
	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/topology" {
			json.NewEncoder(w).Encode(topology())
			return
		}
		if !serveNodeState("node-a", w, r) {
			t.Fatalf("unexpected node-a request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer nodeA.Close()
	nodeAURL = nodeA.URL

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", nodeA.URL,
		"cluster", "upgrade-plan",
		"-target-version", "v1.3.0",
		"-max-backup-age", "2h",
	}, stdout, &bytes.Buffer{}, nodeA.Client()); err != nil {
		t.Fatalf("run(cluster upgrade-plan) error = %v", err)
	}
	var result clusterUpgradePlanResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster upgrade-plan result) error = %v", err)
	}
	if !result.OK || !result.Ready || !result.Compatible || result.TargetVersion != "v1.3.0" || len(result.Nodes) != 2 {
		t.Fatalf("upgrade plan result = %#v, want ready compatible plan", result)
	}
	if result.Nodes[0].ID != "node-b" || result.Nodes[0].Primary || result.Nodes[1].ID != "node-a" || !result.Nodes[1].Primary {
		t.Fatalf("upgrade order = %#v, want replica before primary", result.Nodes)
	}
	if len(result.Nodes[0].CanaryChecks) < 3 || result.Nodes[0].BackupAgeMillis <= 0 || len(result.Nodes[0].Commands) < 2 {
		t.Fatalf("node-b upgrade step = %#v, want backup age, commands, and canaries", result.Nodes[0])
	}
}

func TestLatestSuccessfulBackupRejectsStaleOrMalformedEvents(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	events := []hatriecache.AuditEvent{
		{Time: "bad", Action: "backup", OK: true},
		{Time: now.Add(-time.Hour).Format(time.RFC3339Nano), Action: "backup", OK: false},
		{Time: now.Add(-2 * time.Hour).Format(time.RFC3339Nano), Action: "backup", OK: true},
	}
	got, ok := latestSuccessfulBackup(events)
	if !ok || !got.Equal(now.Add(-2*time.Hour)) {
		t.Fatalf("latestSuccessfulBackup() = %v/%v", got, ok)
	}
}

func TestClusterMaintenanceTopologyIsIdempotent(t *testing.T) {
	topology := hatriecache.ClusterTopology{
		Version: 1,
		Mode:    hatriecache.TopologyModeFullReplica,
		Self:    "node-a",
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Role: "primary"},
			{ID: "node-b", Role: "replica"},
		},
	}
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	maintained, changed, err := clusterMaintenanceTopology(topology, "node-a", true, "kernel upgrade", now)
	if err != nil || !changed {
		t.Fatalf("clusterMaintenanceTopology(begin) changed/error = %v/%v", changed, err)
	}
	if !maintained.Nodes[0].Maintenance || maintained.Nodes[0].MaintenanceReason != "kernel upgrade" || maintained.Nodes[0].MaintenanceSince != now.Format(time.RFC3339) {
		t.Fatalf("maintenance node = %#v", maintained.Nodes[0])
	}
	again, changed, err := clusterMaintenanceTopology(maintained, "node-a", true, "kernel upgrade", now.Add(time.Hour))
	if err != nil || changed || !reflect.DeepEqual(again, maintained) {
		t.Fatalf("clusterMaintenanceTopology(retry) changed/error/topology = %v/%v/%#v", changed, err, again)
	}
	restored, changed, err := clusterMaintenanceTopology(maintained, "node-a", false, "", now.Add(time.Hour))
	if err != nil || !changed || restored.Nodes[0].Maintenance || restored.Nodes[0].MaintenanceReason != "" || restored.Nodes[0].MaintenanceSince != "" {
		t.Fatalf("clusterMaintenanceTopology(end) changed/error/node = %v/%v/%#v", changed, err, restored.Nodes[0])
	}
}

func TestValidateClusterMaintenanceBeginRejectsMissingFailover(t *testing.T) {
	topology := hatriecache.ClusterTopology{
		Version: 1,
		Mode:    hatriecache.TopologyModeSharded,
		Nodes:   []hatriecache.TopologyNode{{ID: "node-a", Role: "primary"}},
		Shards:  []hatriecache.TopologyShard{{ID: 0, Primary: "node-a"}},
	}
	election := hatriecache.ElectionStatus{
		Nodes:   []hatriecache.ElectionNodeStatus{{ID: "node-a", Online: true}},
		Leaders: []hatriecache.ElectionLeader{{Shard: 0, Leader: "node-a", Available: true, Candidates: []string{"node-a"}}},
	}
	if err := validateClusterMaintenanceBegin(topology, election, "node-a"); err == nil || !strings.Contains(err.Error(), "no online failover") {
		t.Fatalf("validateClusterMaintenanceBegin() error = %v, want failover refusal", err)
	}
}

func TestRunClusterMaintenanceBeginAndEndPersistAndVerify(t *testing.T) {
	var nodeAURL string
	var nodeBURL string
	initial := func(self string) hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeSharded,
			Self:    self,
			Nodes: []hatriecache.TopologyNode{
				{ID: "node-a", Address: nodeAURL, Role: "primary"},
				{ID: "node-b", Address: nodeBURL, Role: "replica"},
			},
			Shards: []hatriecache.TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
		}
	}
	var nodeATopology hatriecache.ClusterTopology
	var nodeBTopology hatriecache.ClusterTopology
	electionFor := func(topology hatriecache.ClusterTopology) hatriecache.ElectionStatus {
		maintained := false
		for _, node := range topology.Nodes {
			if node.ID == "node-a" {
				maintained = node.Maintenance
			}
		}
		leader := "node-a"
		reason := "healthy"
		if maintained {
			leader = "node-b"
			reason = "maintenance"
		}
		return hatriecache.ElectionStatus{
			Nodes: []hatriecache.ElectionNodeStatus{
				{ID: "node-a", Online: !maintained, Reason: reason},
				{ID: "node-b", Online: true, Reason: "healthy"},
			},
			Leaders: []hatriecache.ElectionLeader{{Shard: 0, Leader: leader, Available: true, Primary: "node-a", Candidates: []string{"node-a", "node-b"}}},
		}
	}

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "PUT /api/topology":
			nodeBTopology = hatriecache.ClusterTopology{}
			json.NewDecoder(r.Body).Decode(&nodeBTopology)
			json.NewEncoder(w).Encode(nodeBTopology)
		case "GET /api/topology":
			if len(nodeBTopology.Nodes) == 0 {
				json.NewEncoder(w).Encode(initial("node-b"))
			} else {
				json.NewEncoder(w).Encode(nodeBTopology)
			}
		case "GET /api/election":
			if len(nodeBTopology.Nodes) == 0 {
				json.NewEncoder(w).Encode(electionFor(initial("node-b")))
			} else {
				json.NewEncoder(w).Encode(electionFor(nodeBTopology))
			}
		default:
			t.Fatalf("unexpected node-b request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer nodeB.Close()
	nodeBURL = nodeB.URL

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /api/topology":
			if len(nodeATopology.Nodes) == 0 {
				json.NewEncoder(w).Encode(initial("node-a"))
			} else {
				json.NewEncoder(w).Encode(nodeATopology)
			}
		case "PUT /api/topology":
			nodeATopology = hatriecache.ClusterTopology{}
			json.NewDecoder(r.Body).Decode(&nodeATopology)
			json.NewEncoder(w).Encode(nodeATopology)
		case "GET /api/election":
			if len(nodeATopology.Nodes) == 0 {
				json.NewEncoder(w).Encode(electionFor(initial("node-a")))
			} else {
				json.NewEncoder(w).Encode(electionFor(nodeATopology))
			}
		default:
			t.Fatalf("unexpected node-a request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer nodeA.Close()
	nodeAURL = nodeA.URL

	for _, operation := range []struct {
		name       string
		enabled    bool
		wantLeader string
	}{
		{name: "begin", enabled: true, wantLeader: "node-b"},
		{name: "end", enabled: false, wantLeader: "node-a"},
	} {
		stdout := &bytes.Buffer{}
		args := []string{"-addr", nodeA.URL, "cluster", "maintenance", operation.name, "-node", "node-a"}
		if operation.enabled {
			args = append(args, "-reason", "kernel upgrade")
		}
		if err := run(context.Background(), args, stdout, &bytes.Buffer{}, nodeA.Client()); err != nil {
			t.Fatalf("run(cluster maintenance %s) error = %v", operation.name, err)
		}
		var result clusterMaintenanceResult
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("Unmarshal(cluster maintenance %s) error = %v", operation.name, err)
		}
		if !result.OK || result.Maintenance != operation.enabled || !result.TopologyVerified || !result.ElectionVerified || result.LeaderAfter[0].Leader != operation.wantLeader {
			t.Fatalf("maintenance %s result = %#v", operation.name, result)
		}
	}
}

func TestRunClusterJoinUpdatesPeerTargetAndPullsJournal(t *testing.T) {
	initialTopology := hatriecache.ClusterTopology{
		Version: 1,
		Mode:    hatriecache.TopologyModeSharded,
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080", Role: "primary"},
		},
		Shards: []hatriecache.TopologyShard{
			{ID: 0, Primary: "node-a"},
		},
	}
	var peerTopology hatriecache.ClusterTopology
	var targetTopology hatriecache.ClusterTopology
	var journalPull struct {
		Source       string `json:"source"`
		UntilCurrent bool   `json:"until_current"`
	}

	var peerURL string
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /api/health":
			w.Write([]byte(`{"status":"online"}`))
		case "GET /api/topology":
			if err := json.NewEncoder(w).Encode(initialTopology); err != nil {
				t.Fatalf("Encode(topology) error = %v", err)
			}
		case "PUT /api/topology":
			if err := json.NewDecoder(r.Body).Decode(&peerTopology); err != nil {
				t.Fatalf("Decode(peer topology) error = %v", err)
			}
			w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("unexpected peer request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer peer.Close()
	peerURL = peer.URL

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "PUT /api/topology":
			if err := json.NewDecoder(r.Body).Decode(&targetTopology); err != nil {
				t.Fatalf("Decode(target topology) error = %v", err)
			}
			w.Write([]byte(`{"ok":true}`))
		case "POST /api/journal":
			if err := json.NewDecoder(r.Body).Decode(&journalPull); err != nil {
				t.Fatalf("Decode(journal pull) error = %v", err)
			}
			w.Write([]byte(`{"applied":1,"applied_through":1}`))
		default:
			t.Fatalf("unexpected target request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer target.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", peerURL,
		"cluster", "join",
		"-node", "node-b",
		"-address", target.URL,
	}, stdout, &bytes.Buffer{}, peer.Client()); err != nil {
		t.Fatalf("run(cluster join) error = %v", err)
	}
	if len(peerTopology.Nodes) != 2 || len(peerTopology.Shards) != 1 || !reflect.DeepEqual(peerTopology.Shards[0].Replicas, []string{"node-b"}) {
		t.Fatalf("peer topology = %#v, want node-b replica", peerTopology)
	}
	if targetTopology.Self != "node-b" {
		t.Fatalf("target topology self = %q, want node-b", targetTopology.Self)
	}
	targetTopology.Self = peerTopology.Self
	if !reflect.DeepEqual(targetTopology, peerTopology) {
		t.Fatalf("target topology = %#v, want peer topology with node-local self %#v", targetTopology, peerTopology)
	}
	if journalPull.Source != peerURL || !journalPull.UntilCurrent {
		t.Fatalf("journal pull = %#v, want source peer until_current", journalPull)
	}
	var result clusterJoinResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster join result) error = %v", err)
	}
	if !result.OK || !result.TopologyUpdated || !result.TargetUpdated || !result.JournalPulled {
		t.Fatalf("cluster join result = %#v, want all steps completed", result)
	}
}

func TestRunClusterJoinRequiresNodeAndAddress(t *testing.T) {
	err := run(context.Background(), []string{"cluster", "join", "-address", "http://node-b:8080"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "cluster join -node is required") {
		t.Fatalf("run(cluster join without node) error = %v, want node requirement", err)
	}
	err = run(context.Background(), []string{"cluster", "join", "-node", "node-b"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "cluster join -address is required") {
		t.Fatalf("run(cluster join without address) error = %v, want address requirement", err)
	}
}

func TestRunClusterAddReplicaStagesCatchupBeforeActivation(t *testing.T) {
	var peerURL string
	var targetURL string
	initial := func() hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeSharded,
			Self:    "node-a",
			Nodes:   []hatriecache.TopologyNode{{ID: "node-a", Address: peerURL, Role: "primary"}},
			Shards:  []hatriecache.TopologyShard{{ID: 0, Primary: "node-a"}},
		}
	}
	var sequence []string
	var peerTopology hatriecache.ClusterTopology
	var targetTopology hatriecache.ClusterTopology
	var topologyReads int
	var finalSync bool

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-a", APIVersion: hatriecache.MonitoringAPIVersion})
		case "GET /api/topology":
			topologyReads++
			if len(peerTopology.Nodes) == 0 {
				json.NewEncoder(w).Encode(initial())
			} else {
				json.NewEncoder(w).Encode(peerTopology)
			}
		case "PUT /api/topology":
			sequence = append(sequence, "activate-peer")
			if err := json.NewDecoder(r.Body).Decode(&peerTopology); err != nil {
				t.Fatalf("Decode(peer topology) error = %v", err)
			}
			json.NewEncoder(w).Encode(peerTopology)
		case "POST /api/replication":
			sequence = append(sequence, "final-sync")
			finalSync = true
			json.NewEncoder(w).Encode(hatriecache.ReplicationResult{Health: "healthy", HealthScore: 100})
		default:
			t.Fatalf("unexpected peer request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer peer.Close()
	peerURL = peer.URL

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-b", APIVersion: hatriecache.MonitoringAPIVersion})
		case "GET /api/storage":
			w.Write([]byte(`{"configured":true,"store":"pebble"}`))
		case "POST /api/journal":
			sequence = append(sequence, "catch-up")
			var request struct {
				Source       string `json:"source"`
				UntilCurrent bool   `json:"until_current"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("Decode(journal request) error = %v", err)
			}
			if request.Source != peerURL || !request.UntilCurrent {
				t.Fatalf("journal request = %#v, want peer until current", request)
			}
			w.Write([]byte(`{"applied":3,"applied_through":3}`))
		case "PUT /api/topology":
			sequence = append(sequence, "activate-target")
			if err := json.NewDecoder(r.Body).Decode(&targetTopology); err != nil {
				t.Fatalf("Decode(target topology) error = %v", err)
			}
			json.NewEncoder(w).Encode(targetTopology)
		case "GET /api/topology":
			json.NewEncoder(w).Encode(targetTopology)
		default:
			t.Fatalf("unexpected target request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer target.Close()
	targetURL = target.URL

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", peerURL,
		"cluster", "add-replica",
		"-address", targetURL,
	}, stdout, &bytes.Buffer{}, peer.Client()); err != nil {
		t.Fatalf("run(cluster add-replica) error = %v", err)
	}
	if topologyReads < 3 {
		t.Fatalf("topology reads = %d, want initial, post-catch-up, and verification reads", topologyReads)
	}
	if len(sequence) < 4 || sequence[0] != "catch-up" || sequence[1] != "activate-peer" || sequence[2] != "activate-target" || sequence[3] != "final-sync" {
		t.Fatalf("operation sequence = %#v, want catch-up before activation and final sync", sequence)
	}
	if !finalSync || peerTopology.Self != "node-a" || targetTopology.Self != "node-b" {
		t.Fatalf("final state sync=%v peer-self=%q target-self=%q", finalSync, peerTopology.Self, targetTopology.Self)
	}
	var result clusterAddReplicaResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster add-replica result) error = %v", err)
	}
	if !result.OK || result.Node != "node-b" || !result.JournalPulled || !result.FinalSync || !result.TopologyVerified || !reflect.DeepEqual(result.NodesUpdated, []string{"node-a", "node-b"}) {
		t.Fatalf("cluster add-replica result = %#v, want complete staged workflow", result)
	}
}

func TestRunClusterAddReplicaRejectsTargetIdentityMismatch(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-other"})
	}))
	defer target.Close()
	err := run(context.Background(), []string{
		"cluster", "add-replica",
		"-address", target.URL,
		"-node", "node-b",
	}, &bytes.Buffer{}, &bytes.Buffer{}, target.Client())
	if err == nil || !strings.Contains(err.Error(), "reports node") {
		t.Fatalf("run(cluster add-replica identity mismatch) error = %v, want mismatch", err)
	}
}

func TestRunClusterRemoveUpdatesRemainingNodes(t *testing.T) {
	var peerURL string
	var remainingURL string
	topology := func() hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeSharded,
			Self:    "node-a",
			Nodes: []hatriecache.TopologyNode{
				{ID: "node-a", Address: peerURL, Role: "primary"},
				{ID: "node-b", Address: "http://removed.invalid", Role: "replica"},
				{ID: "node-c", Address: remainingURL, Role: "replica"},
			},
			Shards: []hatriecache.TopologyShard{
				{ID: 0, Primary: "node-a", Replicas: []string{"node-b", "node-c"}},
			},
		}
	}

	var peerTopology hatriecache.ClusterTopology
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /api/health":
			w.Write([]byte(`{"status":"online"}`))
		case "GET /api/topology":
			json.NewEncoder(w).Encode(topology())
		case "PUT /api/topology":
			if err := json.NewDecoder(r.Body).Decode(&peerTopology); err != nil {
				t.Fatalf("Decode(peer topology) error = %v", err)
			}
			w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("unexpected peer request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer peer.Close()
	peerURL = peer.URL

	var remainingTopology hatriecache.ClusterTopology
	remaining := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/topology" {
			t.Fatalf("unexpected remaining-node request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&remainingTopology); err != nil {
			t.Fatalf("Decode(remaining topology) error = %v", err)
		}
		w.Write([]byte(`{"ok":true}`))
	}))
	defer remaining.Close()
	remainingURL = remaining.URL

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", peerURL,
		"cluster", "remove",
		"-node", "node-b",
	}, stdout, &bytes.Buffer{}, peer.Client()); err != nil {
		t.Fatalf("run(cluster remove) error = %v", err)
	}
	if peerTopology.Self != "node-a" || remainingTopology.Self != "node-c" {
		t.Fatalf("topology self values = peer %q remaining %q, want node-a/node-c", peerTopology.Self, remainingTopology.Self)
	}
	for _, updated := range []hatriecache.ClusterTopology{peerTopology, remainingTopology} {
		if len(updated.Nodes) != 2 || len(updated.Shards) != 1 || !reflect.DeepEqual(updated.Shards[0].Replicas, []string{"node-c"}) {
			t.Fatalf("updated topology = %#v, want node-b removed", updated)
		}
	}
	var result clusterRemoveResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster remove result) error = %v", err)
	}
	if !result.OK || !result.TopologyUpdated || !reflect.DeepEqual(result.NodesUpdated, []string{"node-a", "node-c"}) {
		t.Fatalf("cluster remove result = %#v, want both remaining nodes updated", result)
	}
}

func TestRunClusterRemoveRequiresNode(t *testing.T) {
	err := run(context.Background(), []string{"cluster", "remove"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "cluster remove -node is required") {
		t.Fatalf("run(cluster remove without node) error = %v, want node requirement", err)
	}
}

func TestRunClusterDecommissionValidatesSyncsAndRemoves(t *testing.T) {
	var peerURL string
	var targetURL string
	var survivorURL string
	currentTopology := func() hatriecache.ClusterTopology {
		return hatriecache.ClusterTopology{
			Version: 1,
			Mode:    hatriecache.TopologyModeSharded,
			Self:    "node-a",
			Nodes: []hatriecache.TopologyNode{
				{ID: "node-a", Address: peerURL, Role: "primary"},
				{ID: "node-b", Address: targetURL, Role: "replica"},
				{ID: "node-c", Address: survivorURL, Role: "replica"},
			},
			Shards: []hatriecache.TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b", "node-c"}}},
		}
	}
	var peerTopology hatriecache.ClusterTopology
	var survivorTopology hatriecache.ClusterTopology
	var sequence []string

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-a"})
		case "GET /api/topology":
			if len(peerTopology.Nodes) == 0 {
				json.NewEncoder(w).Encode(currentTopology())
			} else {
				json.NewEncoder(w).Encode(peerTopology)
			}
		case "POST /api/replication":
			sequence = append(sequence, "final-sync")
			json.NewEncoder(w).Encode(hatriecache.ReplicationResult{Health: "healthy", HealthScore: 100})
		case "POST /api/election":
			sequence = append(sequence, "offline")
			var request struct {
				Node   string `json:"node"`
				Online bool   `json:"online"`
			}
			json.NewDecoder(r.Body).Decode(&request)
			if request.Node != "node-b" || request.Online {
				t.Fatalf("election request = %#v, want node-b offline", request)
			}
			w.Write([]byte(`{"ok":true}`))
		case "PUT /api/topology":
			sequence = append(sequence, "remove-peer")
			json.NewDecoder(r.Body).Decode(&peerTopology)
			json.NewEncoder(w).Encode(peerTopology)
		default:
			t.Fatalf("unexpected peer request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer peer.Close()
	peerURL = peer.URL

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/health" {
			t.Fatalf("removed target unexpectedly contacted with %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-b"})
	}))
	defer target.Close()
	targetURL = target.URL

	survivor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /api/health":
			json.NewEncoder(w).Encode(hatriecache.MonitoringHealth{Status: "online", Node: "node-c"})
		case "PUT /api/topology":
			sequence = append(sequence, "remove-survivor")
			json.NewDecoder(r.Body).Decode(&survivorTopology)
			json.NewEncoder(w).Encode(survivorTopology)
		case "GET /api/topology":
			json.NewEncoder(w).Encode(survivorTopology)
		default:
			t.Fatalf("unexpected survivor request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer survivor.Close()
	survivorURL = survivor.URL

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-addr", peerURL,
		"cluster", "decommission",
		"-node", "node-b",
	}, stdout, &bytes.Buffer{}, peer.Client()); err != nil {
		t.Fatalf("run(cluster decommission) error = %v", err)
	}
	if !reflect.DeepEqual(sequence, []string{"final-sync", "offline", "remove-peer", "remove-survivor"}) {
		t.Fatalf("decommission sequence = %#v", sequence)
	}
	for _, topology := range []hatriecache.ClusterTopology{peerTopology, survivorTopology} {
		if len(topology.Nodes) != 2 || len(topology.Shards) != 1 || !reflect.DeepEqual(topology.Shards[0].Replicas, []string{"node-c"}) {
			t.Fatalf("decommissioned topology = %#v, want node-b removed", topology)
		}
	}
	var result clusterDecommissionResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal(cluster decommission result) error = %v", err)
	}
	if !result.OK || !result.FinalSync || !result.MarkedOffline || !result.TopologyVerified || result.RemovedAddress != targetURL {
		t.Fatalf("cluster decommission result = %#v, want completed workflow", result)
	}
	if !strings.Contains(result.Cleanup, "node-b") {
		t.Fatalf("cleanup = %q, want node-b instruction", result.Cleanup)
	}
}

func TestRunClusterDecommissionRequiresNode(t *testing.T) {
	err := run(context.Background(), []string{"cluster", "decommission"}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "cluster decommission -node is required") {
		t.Fatalf("run(cluster decommission without node) error = %v, want node requirement", err)
	}
}

func TestRunReportsServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad", http.StatusBadRequest)
	}))
	defer server.Close()

	err := run(context.Background(), []string{"-addr", server.URL, "health"}, &bytes.Buffer{}, &bytes.Buffer{}, server.Client())
	if err == nil || !strings.Contains(err.Error(), "400 Bad Request") {
		t.Fatalf("run(health) error = %v, want server error", err)
	}
}
