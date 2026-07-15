package main

import (
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
	cfg, remaining, err := parseGlobalFlags([]string{"-addr", "http://cache", "-timeout", "250ms", "stats"}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseGlobalFlags() error = %v", err)
	}
	if cfg.addr != "http://cache" {
		t.Fatalf("addr = %q, want http://cache", cfg.addr)
	}
	if cfg.timeout != 250*time.Millisecond {
		t.Fatalf("timeout = %s, want 250ms", cfg.timeout)
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
		w.Write([]byte(`{"skipped":true,"reason":"none"}`))
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{"-addr", server.URL, "replication"}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(replication) error = %v", err)
	}
	if gotPath != "/api/replication" {
		t.Fatalf("path = %q, want /api/replication", gotPath)
	}
	if got := stdout.String(); got != "{\"skipped\":true,\"reason\":\"none\"}\n" {
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
	}, stdout, &bytes.Buffer{}, server.Client()); err != nil {
		t.Fatalf("run(journal -pull-from) error = %v", err)
	}
	if gotPath != "/api/journal" {
		t.Fatalf("path = %q, want /api/journal", gotPath)
	}
	if gotBody.Source != "http://leader" || gotBody.AfterSequence != 7 || gotBody.Limit != 25 || !gotBody.UntilCurrent || gotBody.MaxBatches != 3 {
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
