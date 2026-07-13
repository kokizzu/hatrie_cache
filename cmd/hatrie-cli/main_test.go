package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	hatriecache "hatrie_cache"
)

func TestRunRequiresSubcommand(t *testing.T) {
	err := run(context.Background(), nil, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "subcommand is required") {
		t.Fatalf("run() error = %v, want subcommand error", err)
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

func TestRunCommandPostsJSON(t *testing.T) {
	var gotRequest hatriecache.CacheCommandRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
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
