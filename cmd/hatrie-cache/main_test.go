package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	hatriecache "hatrie_cache"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestParseConfigDefaultsMonitoringServerOff(t *testing.T) {
	cfg, err := parseConfig(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.monitoringServer {
		t.Fatal("monitoringServer = true, want false")
	}
	if cfg.monitoringAddr != "127.0.0.1:8080" {
		t.Fatalf("monitoringAddr = %q, want default", cfg.monitoringAddr)
	}
	if cfg.replicationWireFormat != string(hatriecache.DefaultCommandWireFormat) {
		t.Fatalf("replicationWireFormat = %q, want default", cfg.replicationWireFormat)
	}
	if cfg.dbFormat != string(hatriecache.DefaultStorageFormat) {
		t.Fatalf("dbFormat = %q, want default", cfg.dbFormat)
	}
	if cfg.snapshotFormat != string(hatriecache.DefaultSnapshotFormat) {
		t.Fatalf("snapshotFormat = %q, want default", cfg.snapshotFormat)
	}
	if cfg.journalFormat != string(hatriecache.DefaultCommandJournalFormat) {
		t.Fatalf("journalFormat = %q, want default", cfg.journalFormat)
	}
	if cfg.monitoringReadHeaderTimeout != defaultMonitoringReadHeaderTimeout {
		t.Fatalf("monitoring read header timeout = %s, want %s", cfg.monitoringReadHeaderTimeout, defaultMonitoringReadHeaderTimeout)
	}
	if cfg.monitoringIdleTimeout != defaultMonitoringIdleTimeout {
		t.Fatalf("monitoring idle timeout = %s, want %s", cfg.monitoringIdleTimeout, defaultMonitoringIdleTimeout)
	}
}

func TestRunMonitoringDisabledAcceptsNilWriters(t *testing.T) {
	if err := run(context.Background(), nil, nil, nil); err != nil {
		t.Fatalf("run(disabled, nil writers) error = %v", err)
	}
}

func TestParseConfigEnablesMonitoringServerExplicitly(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-check-config",
		"-monitoring-server",
		"-monitoring-addr", "127.0.0.1:9090",
		"-monitoring-web-dir", "/tmp/web",
		"-monitoring-auth-token", "secret",
		"-audit-log-path", "/tmp/audit.jsonl",
		"-write-protection",
		"-rate-limit", "7",
		"-monitoring-read-header-timeout", "750ms",
		"-monitoring-idle-timeout", "15s",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if !cfg.monitoringServer {
		t.Fatal("monitoringServer = false, want true")
	}
	if !cfg.checkConfig {
		t.Fatal("checkConfig = false, want true")
	}
	if cfg.monitoringAddr != "127.0.0.1:9090" || cfg.monitoringWebDir != "/tmp/web" || cfg.monitoringAuthToken != "secret" || cfg.auditLogPath != "/tmp/audit.jsonl" {
		t.Fatalf("cfg = %#v, want explicit address and web dir", cfg)
	}
	if !cfg.writeProtection || cfg.rateLimit != 7 {
		t.Fatalf("write protection/rate limit = %v/%d, want true/7", cfg.writeProtection, cfg.rateLimit)
	}
	if cfg.monitoringReadHeaderTimeout != 750*time.Millisecond || cfg.monitoringIdleTimeout != 15*time.Second {
		t.Fatalf("monitoring timeouts = %s/%s, want 750ms/15s", cfg.monitoringReadHeaderTimeout, cfg.monitoringIdleTimeout)
	}
}

func TestParseConfigLoadsConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hatrie-cache.json")
	if err := os.WriteFile(path, []byte(`{
		"monitoring_server": true,
		"monitoring_addr": "0.0.0.0:18080",
		"monitoring_web_dir": "/srv/hatrie-cache/web",
		"monitoring_auth_token": "secret",
		"monitoring_read_header_timeout": "9s",
		"monitoring_idle_timeout": "45s",
		"node_id": "node-a",
		"grpc_addr": "0.0.0.0:19090",
		"replication": true,
		"replication_mode": "command",
		"replication_async": true,
		"replication_queue_size": 16,
		"replication_retry_interval": "10ms",
		"replication_max_attempts": 2,
		"replication_dead_letter_limit": 9,
		"replication_outbox_path": "/data/replication-outbox.json",
		"replication_outbox_format": "json",
		"replication_circuit_breaker_failures": 4,
		"replication_circuit_breaker_cooldown": "12s",
		"replication_auth_token": "replica-secret",
		"replication_batch_max_bytes": 4096,
		"db_path": "/data/cache.leveldb",
		"db_hot_load": true,
		"db_hot_load_max_bytes": 2048,
		"snapshot_interval": "30s",
		"journal_pull_limit": 123
	}`), 0o600); err != nil {
		t.Fatalf("WriteFile(config) error = %v", err)
	}

	cfg, err := parseConfig([]string{"-config", path}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig(config file) error = %v", err)
	}
	if cfg.configPath != path || !cfg.monitoringServer || cfg.monitoringAddr != "0.0.0.0:18080" || cfg.monitoringWebDir != "/srv/hatrie-cache/web" || cfg.monitoringAuthToken != "secret" {
		t.Fatalf("monitoring config = %#v, want file values", cfg)
	}
	if cfg.monitoringReadHeaderTimeout != 9*time.Second || cfg.monitoringIdleTimeout != 45*time.Second {
		t.Fatalf("monitoring timeouts = %s/%s, want 9s/45s", cfg.monitoringReadHeaderTimeout, cfg.monitoringIdleTimeout)
	}
	if cfg.nodeID != "node-a" || cfg.grpcAddr != "0.0.0.0:19090" {
		t.Fatalf("node/grpc config = %q/%q, want node-a/grpc addr", cfg.nodeID, cfg.grpcAddr)
	}
	if !cfg.replication || cfg.replicationMode != replicationModeCommand || !cfg.replicationAsync || cfg.replicationQueueSize != 16 || cfg.replicationRetry != 10*time.Millisecond || cfg.replicationAttempts != 2 || cfg.replicationDeadLetterLimit != 9 {
		t.Fatalf("replication config = %#v, want file values", cfg)
	}
	if cfg.replicationOutboxPath != "/data/replication-outbox.json" || cfg.replicationOutboxFormat != "json" {
		t.Fatalf("replication outbox config = %q/%q, want config file values", cfg.replicationOutboxPath, cfg.replicationOutboxFormat)
	}
	if cfg.replicationCircuitFailures != 4 || cfg.replicationCircuitCooldown != 12*time.Second {
		t.Fatalf("replication circuit breaker config = %d/%s, want 4/12s", cfg.replicationCircuitFailures, cfg.replicationCircuitCooldown)
	}
	if cfg.replicationAuthToken != "replica-secret" {
		t.Fatalf("replication auth token = %q, want config file value", cfg.replicationAuthToken)
	}
	if cfg.replicationBatchMaxBytes != 4096 {
		t.Fatalf("replication batch max bytes = %d, want config file value", cfg.replicationBatchMaxBytes)
	}
	if cfg.dbPath != "/data/cache.leveldb" || !cfg.dbHotLoad || cfg.dbHotLoadMaxBytes != 2048 {
		t.Fatalf("db config = %#v, want file values", cfg)
	}
	if cfg.snapshotInterval != 30*time.Second || cfg.journalPullLimit != 123 {
		t.Fatalf("snapshot/journal config = %s/%d, want 30s/123", cfg.snapshotInterval, cfg.journalPullLimit)
	}
}

func TestParseConfigCLIOverridesConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hatrie-cache.json")
	if err := os.WriteFile(path, []byte(`{
		"monitoring_server": false,
		"monitoring_addr": "0.0.0.0:18080",
		"replication": false
	}`), 0o600); err != nil {
		t.Fatalf("WriteFile(config) error = %v", err)
	}

	cfg, err := parseConfig([]string{
		"-config", path,
		"-monitoring-server",
		"-monitoring-addr", "127.0.0.1:19090",
		"-replication=true",
		"-replication-mode", "command",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig(config override) error = %v", err)
	}
	if !cfg.monitoringServer || cfg.monitoringAddr != "127.0.0.1:19090" || !cfg.replication {
		t.Fatalf("cfg = %#v, want CLI values to override config file", cfg)
	}
}

func TestParseConfigRejectsInvalidConfigFile(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "unknown option",
			body: `{"not_real": true}`,
			want: `unknown option "not_real"`,
		},
		{
			name: "bad value type",
			body: `{"monitoring_server": {"enabled": true}}`,
			want: "value must be a string, bool, or number",
		},
		{
			name: "bad duration",
			body: `{"snapshot_interval": "often"}`,
			want: "parse error",
		},
		{
			name: "nested config",
			body: `{"config": "other.json"}`,
			want: `must be provided on the command line`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "hatrie-cache.json")
			if err := os.WriteFile(path, []byte(tt.body), 0o600); err != nil {
				t.Fatalf("WriteFile(config) error = %v", err)
			}
			_, err := parseConfig([]string{"-config", path}, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parseConfig(%s) error = %v, want %q", tt.name, err, tt.want)
			}
		})
	}
}

func TestParseConfigRejectsMissingConfigValue(t *testing.T) {
	_, err := parseConfig([]string{"-config"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "-config requires a value") {
		t.Fatalf("parseConfig(missing config) error = %v, want missing value", err)
	}
}

func TestParseConfigRejectsNegativeMonitoringTimeouts(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "read header",
			args: []string{"-monitoring-read-header-timeout", "-1s"},
			want: "monitoring read header timeout must be non-negative",
		},
		{
			name: "idle",
			args: []string{"-monitoring-idle-timeout", "-1s"},
			want: "monitoring idle timeout must be non-negative",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseConfig(tt.args, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parseConfig(%s) error = %v, want %q", tt.name, err, tt.want)
			}
		})
	}
}

func TestParseConfigRejectsNegativeRateLimit(t *testing.T) {
	_, err := parseConfig([]string{"-rate-limit", "-1"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "rate limit must be non-negative") {
		t.Fatalf("parseConfig(negative rate limit) error = %v, want rate limit rejection", err)
	}
}

func TestNewMonitoringServerAppliesTimeouts(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	cfg := config{
		monitoringAddr:              "127.0.0.1:9090",
		monitoringReadHeaderTimeout: 2 * time.Second,
		monitoringIdleTimeout:       30 * time.Second,
	}
	server := newMonitoringServer(cfg, handler)

	if server.Addr != cfg.monitoringAddr {
		t.Fatalf("server Addr = %q, want %q", server.Addr, cfg.monitoringAddr)
	}
	if server.Handler == nil {
		t.Fatal("server Handler = nil")
	}
	if server.ReadHeaderTimeout != 2*time.Second || server.IdleTimeout != 30*time.Second {
		t.Fatalf("server timeouts = %s/%s, want 2s/30s", server.ReadHeaderTimeout, server.IdleTimeout)
	}
	if server.TLSConfig == nil || !containsString(server.TLSConfig.NextProtos, "h2") {
		t.Fatalf("server TLSConfig NextProtos = %#v, want h2 enabled", server.TLSConfig)
	}
}

func TestParseConfigSnapshotFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-snapshot-path", "/tmp/cache.json",
		"-snapshot-interval", "5s",
		"-snapshot-format", "json",
		"-journal-path", "/tmp/cache.journal",
		"-journal-format", "json",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.snapshotPath != "/tmp/cache.json" || cfg.snapshotInterval != 5*time.Second {
		t.Fatalf("cfg snapshot = %q/%s, want explicit values", cfg.snapshotPath, cfg.snapshotInterval)
	}
	if cfg.snapshotFormat != "json" || snapshotFormat(cfg) != hatriecache.SnapshotFormatJSON {
		t.Fatalf("snapshot format = %q, want json", cfg.snapshotFormat)
	}
	if cfg.journalPath != "/tmp/cache.journal" {
		t.Fatalf("journalPath = %q, want explicit path", cfg.journalPath)
	}
	if cfg.journalFormat != "json" || journalFormat(cfg) != hatriecache.CommandJournalFormatJSON {
		t.Fatalf("journal format = %q, want json", cfg.journalFormat)
	}
}

func TestParseConfigJournalPullFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-journal-path", "/tmp/cache.journal",
		"-journal-pull-source", "http://leader:8080",
		"-journal-pull-state-path", "/tmp/cache.pull.json",
		"-journal-pull-interval", "5s",
		"-journal-pull-timeout", "750ms",
		"-journal-pull-limit", "250",
		"-journal-pull-max-batches", "8",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.journalPullSource != "http://leader:8080" || cfg.journalPullStatePath != "/tmp/cache.pull.json" || cfg.journalPullInterval != 5*time.Second {
		t.Fatalf("cfg journal pull basics = %#v, want explicit source/state/interval", cfg)
	}
	if cfg.journalPullTimeout != 750*time.Millisecond {
		t.Fatalf("cfg journal pull timeout = %s, want 750ms", cfg.journalPullTimeout)
	}
	if cfg.journalPullLimit != 250 || cfg.journalPullMaxBatches != 8 {
		t.Fatalf("cfg journal pull limits = %d/%d, want 250/8", cfg.journalPullLimit, cfg.journalPullMaxBatches)
	}
}

func TestParseConfigRejectsNegativeJournalPullTimeout(t *testing.T) {
	_, err := parseConfig([]string{
		"-monitoring-server",
		"-journal-pull-timeout", "-1s",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "journal pull timeout must be non-negative") {
		t.Fatalf("parseConfig(negative journal pull timeout) error = %v, want non-negative timeout error", err)
	}
}

func TestParseConfigMonitoringTLSFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-monitoring-tls-cert", "/tmp/cert.pem",
		"-monitoring-tls-key", "/tmp/key.pem",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.monitoringTLSCert != "/tmp/cert.pem" || cfg.monitoringTLSKey != "/tmp/key.pem" {
		t.Fatalf("cfg TLS = %q/%q, want explicit paths", cfg.monitoringTLSCert, cfg.monitoringTLSKey)
	}
	if got := monitoringURL(cfg); got != "https://127.0.0.1:8080" {
		t.Fatalf("monitoringURL() = %q, want https URL", got)
	}
}

func TestParseConfigTopologyFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-node-id", "node-a",
		"-topology-path", "/tmp/topology.json",
		"-election-timeout", "30s",
		"-replication",
		"-replication-mode", "command",
		"-replication-async",
		"-replication-queue-size", "16",
		"-replication-retry-interval", "50ms",
		"-replication-max-attempts", "5",
		"-replication-dead-letter-limit", "7",
		"-replication-outbox-path", "/tmp/replication-outbox.json",
		"-replication-outbox-format", "leveldb",
		"-replication-circuit-breaker-failures", "4",
		"-replication-circuit-breaker-cooldown", "20s",
		"-replication-wire-format", "json",
		"-replication-auth-token", "replica-secret",
		"-replication-batch-max-bytes", "2048",
		"-replication-sync-interval", "10s",
		"-replication-sync-prefix", "session:",
		"-enforce-leader-writes",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.nodeID != "node-a" || cfg.topologyPath != "/tmp/topology.json" || cfg.electionTimeout != 30*time.Second || !cfg.replication || !cfg.enforceLeaderWrites {
		t.Fatalf("cfg topology = %#v, want explicit node and path", cfg)
	}
	if !cfg.replicationAsync || cfg.replicationQueueSize != 16 || cfg.replicationRetry != 50*time.Millisecond || cfg.replicationAttempts != 5 || cfg.replicationDeadLetterLimit != 7 {
		t.Fatalf("cfg replication async = %#v, want explicit async queue settings", cfg)
	}
	if cfg.replicationOutboxPath != "/tmp/replication-outbox.json" || cfg.replicationOutboxFormat != "leveldb" {
		t.Fatalf("cfg replication outbox = %q/%q, want explicit path and format", cfg.replicationOutboxPath, cfg.replicationOutboxFormat)
	}
	if cfg.replicationCircuitFailures != 4 || cfg.replicationCircuitCooldown != 20*time.Second {
		t.Fatalf("cfg replication circuit breaker = %d/%s, want 4/20s", cfg.replicationCircuitFailures, cfg.replicationCircuitCooldown)
	}
	if cfg.replicationWireFormat != "json" || replicationWireFormat(cfg) != hatriecache.CommandWireFormatJSON {
		t.Fatalf("replication wire format = %q, want json", cfg.replicationWireFormat)
	}
	if cfg.replicationAuthToken != "replica-secret" {
		t.Fatalf("replication auth token = %q, want explicit value", cfg.replicationAuthToken)
	}
	if cfg.replicationBatchMaxBytes != 2048 {
		t.Fatalf("replication batch max bytes = %d, want explicit value", cfg.replicationBatchMaxBytes)
	}
	if cfg.replicationSyncInterval != 10*time.Second || cfg.replicationSyncPrefix != "session:" {
		t.Fatalf("cfg replication sync = %s/%q, want 10s/session:", cfg.replicationSyncInterval, cfg.replicationSyncPrefix)
	}
	if got := replicationQueueSize(cfg); got != 16 {
		t.Fatalf("replicationQueueSize(async cfg) = %d, want 16", got)
	}
	cfg.replicationAsync = false
	if got := replicationQueueSize(cfg); got != 0 {
		t.Fatalf("replicationQueueSize(sync cfg) = %d, want 0", got)
	}
}

func TestParseConfigReplicationModeDefaultsToJournal(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-replication",
		"-journal-path", "/tmp/cache.journal",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.replicationMode != replicationModeJournal {
		t.Fatalf("replication mode = %q, want %q", cfg.replicationMode, replicationModeJournal)
	}
	if replicationModeUsesCommandFanout(cfg.replicationMode) {
		t.Fatal("journal replication mode unexpectedly enables command fanout")
	}
	if !replicationModeUsesJournal(cfg.replicationMode) {
		t.Fatal("journal replication mode does not use journal stream")
	}
}

func TestParseConfigReplicationModeCommandKeepsHTTPFanout(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-replication",
		"-replication-mode", "command",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.replicationMode != replicationModeCommand {
		t.Fatalf("replication mode = %q, want %q", cfg.replicationMode, replicationModeCommand)
	}
	if !replicationModeUsesCommandFanout(cfg.replicationMode) {
		t.Fatal("command replication mode does not enable command fanout")
	}
	if replicationModeUsesJournal(cfg.replicationMode) {
		t.Fatal("command replication mode unexpectedly requires journal stream")
	}
}

func TestParseConfigReplicationModeDualUsesJournalAndHTTPFanout(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-replication",
		"-replication-mode", "dual",
		"-journal-path", "/tmp/cache.journal",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.replicationMode != replicationModeDual {
		t.Fatalf("replication mode = %q, want %q", cfg.replicationMode, replicationModeDual)
	}
	if !replicationModeUsesCommandFanout(cfg.replicationMode) {
		t.Fatal("dual replication mode does not enable command fanout")
	}
	if !replicationModeUsesJournal(cfg.replicationMode) {
		t.Fatal("dual replication mode does not use journal stream")
	}
}

func TestParseConfigRejectsInvalidReplicationMode(t *testing.T) {
	_, err := parseConfig([]string{
		"-monitoring-server",
		"-replication-mode", "raft",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "replication mode") {
		t.Fatalf("parseConfig(invalid replication mode) error = %v, want replication mode error", err)
	}
}

func TestParseConfigRejectsJournalReplicationWithoutJournalPath(t *testing.T) {
	_, err := parseConfig([]string{
		"-monitoring-server",
		"-replication",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "journal replication mode requires -journal-path") {
		t.Fatalf("parseConfig(journal replication without journal) error = %v, want journal path error", err)
	}
}

func TestParseConfigRejectsInvalidAsyncReplicationOptions(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "negative queue",
			args: []string{"-replication-queue-size", "-1"},
		},
		{
			name: "async zero queue",
			args: []string{"-replication-async", "-replication-queue-size", "0"},
		},
		{
			name: "async zero attempts",
			args: []string{"-replication-async", "-replication-max-attempts", "0"},
		},
		{
			name: "negative retry",
			args: []string{"-replication-retry-interval", "-1s"},
		},
		{
			name: "outbox without async replication",
			args: []string{"-replication", "-replication-outbox-path", "/tmp/outbox.json"},
		},
		{
			name: "invalid outbox format",
			args: []string{"-replication-outbox-format", "sqlite"},
		},
		{
			name: "negative circuit breaker failures",
			args: []string{"-replication-circuit-breaker-failures", "-1"},
		},
		{
			name: "negative circuit breaker cooldown",
			args: []string{"-replication-circuit-breaker-cooldown", "-1s"},
		},
		{
			name: "negative sync interval",
			args: []string{"-replication", "-replication-sync-interval", "-1s"},
		},
		{
			name: "sync without replication",
			args: []string{"-replication-sync-interval", "1s"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseConfig(tt.args, &bytes.Buffer{}); err == nil {
				t.Fatal("parseConfig() error = nil, want error")
			}
		})
	}
}

func TestStartReplicationSyncerRunsImmediatePrefixSync(t *testing.T) {
	requests := make(chan hatriecache.CacheCommandRequest, 2)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		var request hatriecache.CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode(replication request) error = %v", err)
		}
		requests <- request
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(hatriecache.CacheCommandResponse{OK: true, Message: "ok"}); err != nil {
			t.Fatalf("Encode(replication response) error = %v", err)
		}
	}))
	defer target.Close()

	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("session:1", "value")
	ht.UpsertString("other:1", "ignored")
	topology, err := hatriecache.NewTopologyStore(hatriecache.ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://127.0.0.1:1"},
			{ID: "node-b", Address: target.URL},
		},
		Shards: []hatriecache.TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := hatriecache.NewHTTPReplicator(hatriecache.HTTPReplicatorOptions{
		Self:       "node-a",
		Topology:   topology,
		Client:     target.Client(),
		WireFormat: hatriecache.CommandWireFormatJSON,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startReplicationSyncer(ctx, ht, replicator, time.Hour, "session:", &bytes.Buffer{})
	defer stop()

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "session:1" || request.Value == "" {
			t.Fatalf("replication sync request = %#v, want session INTERNALSET", request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replication sync did not run")
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected replication sync request = %#v", request)
	default:
	}
	var result hatriecache.ReplicationResult
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		result = replicator.LastResult()
		if result.Command == "SYNC" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if result.Skipped || result.Command != "SYNC" || result.Key != "session:" || result.Entries != 1 || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("replication sync last result = %#v, want one synced session entry", result)
	}

	stopped := make(chan struct{})
	go func() {
		stop()
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("replication syncer repeated stop did not return")
	}
}

func TestReplicationSyncResultLogFiltering(t *testing.T) {
	tests := []struct {
		name    string
		result  hatriecache.ReplicationResult
		wantLog bool
		wantMsg string
	}{
		{
			name:    "skipped empty reason",
			result:  hatriecache.ReplicationResult{Skipped: true},
			wantLog: false,
		},
		{
			name:    "skipped no entries",
			result:  hatriecache.ReplicationResult{Skipped: true, Reason: "no entries to sync"},
			wantLog: false,
		},
		{
			name:    "skipped topology",
			result:  hatriecache.ReplicationResult{Skipped: true, Reason: "topology is not configured"},
			wantLog: true,
			wantMsg: "topology is not configured",
		},
		{
			name: "all targets ok",
			result: hatriecache.ReplicationResult{Targets: []hatriecache.ReplicationTargetResult{
				{Node: "node-b", OK: true},
				{Node: "node-c", OK: true},
			}},
			wantLog: false,
		},
		{
			name: "one target failed",
			result: hatriecache.ReplicationResult{Targets: []hatriecache.ReplicationTargetResult{
				{Node: "node-b", OK: true},
				{Node: "node-c", OK: false},
			}},
			wantLog: true,
			wantMsg: "1/2 target deliveries failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := replicationSyncResultNeedsLog(tt.result); got != tt.wantLog {
				t.Fatalf("replicationSyncResultNeedsLog() = %v, want %v", got, tt.wantLog)
			}
			if tt.wantLog {
				if got := replicationSyncResultLogMessage(tt.result); got != tt.wantMsg {
					t.Fatalf("replicationSyncResultLogMessage() = %q, want %q", got, tt.wantMsg)
				}
			}
		})
	}
}

func TestStartReplicationSyncerAcceptsNilStderr(t *testing.T) {
	replicator := hatriecache.NewHTTPReplicator(hatriecache.HTTPReplicatorOptions{})
	defer replicator.Close()

	stop := startReplicationSyncer(context.Background(), nil, replicator, time.Hour, "", nil)
	defer stop()

	waitUntil(t, 2*time.Second, func() bool {
		result := replicator.LastResult()
		return result.Command == "SYNC" && result.Skipped && result.Reason == "trie is not configured"
	})
}

func TestParseConfigGRPCFlag(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-grpc-addr", "127.0.0.1:9091",
		"-grpc-tls-cert", "/tmp/grpc-cert.pem",
		"-grpc-tls-key", "/tmp/grpc-key.pem",
		"-grpc-client-ca", "/tmp/client-ca.pem",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.grpcAddr != "127.0.0.1:9091" {
		t.Fatalf("grpcAddr = %q, want explicit address", cfg.grpcAddr)
	}
	if cfg.grpcTLSCert != "/tmp/grpc-cert.pem" || cfg.grpcTLSKey != "/tmp/grpc-key.pem" || cfg.grpcClientCA != "/tmp/client-ca.pem" {
		t.Fatalf("gRPC TLS config = %q/%q/%q, want explicit paths", cfg.grpcTLSCert, cfg.grpcTLSKey, cfg.grpcClientCA)
	}
}

func TestParseConfigLevelDBFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-db-path", "/tmp/cache.leveldb",
		"-db-format", "json",
		"-db-sync-interval", "10s",
		"-db-compact-interval", "30s",
		"-db-compact-start-key", "alpha",
		"-db-compact-limit-key", "omega",
		"-db-hot-load",
		"-db-hot-load-max-bytes", "2048",
		"-db-hot-load-max-age", "30m",
		"-db-hot-load-min-hits", "42",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.dbPath != "/tmp/cache.leveldb" || cfg.dbSyncInterval != 10*time.Second || cfg.dbCompactInterval != 30*time.Second {
		t.Fatalf("cfg db = %q/%s/%s, want explicit path and intervals", cfg.dbPath, cfg.dbSyncInterval, cfg.dbCompactInterval)
	}
	if cfg.dbCompactStartKey != "alpha" || cfg.dbCompactLimitKey != "omega" {
		t.Fatalf("cfg db compact range = %q/%q, want alpha/omega", cfg.dbCompactStartKey, cfg.dbCompactLimitKey)
	}
	if cfg.dbFormat != "json" || storageFormat(cfg) != hatriecache.StorageFormatJSON {
		t.Fatalf("db format = %q, want json", cfg.dbFormat)
	}
	if !cfg.dbHotLoad || cfg.dbHotLoadMaxBytes != 2048 || cfg.dbHotLoadMaxAge != 30*time.Minute || cfg.dbHotLoadMinHits != 42 {
		t.Fatalf("cfg hot-load = %#v, want explicit hot-load options", cfg)
	}
	policy := levelDBLoadPolicy(cfg)
	if !policy.HotValuesOnly || policy.MaxValueBytes != 2048 || policy.MaxLastHitAge != 30*time.Minute || policy.MinHits != 42 {
		t.Fatalf("levelDBLoadPolicy() = %#v, want explicit hot-load policy", policy)
	}
}

func TestParseConfigRejectsNegativeHotLoadLimits(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "max bytes",
			args: []string{"-db-hot-load-max-bytes", "-1"},
		},
		{
			name: "max age",
			args: []string{"-db-hot-load-max-age", "-1s"},
		},
		{
			name: "compact interval",
			args: []string{"-db-compact-interval", "-1s"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseConfig(tt.args, &bytes.Buffer{}); err == nil {
				t.Fatal("parseConfig() error = nil, want error")
			}
		})
	}
}

func TestParseConfigRejectsInvalidDBFormat(t *testing.T) {
	_, err := parseConfig([]string{
		"-monitoring-server",
		"-db-format", "msgpack",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "unsupported storage format") {
		t.Fatalf("parseConfig(invalid db format) error = %v, want unsupported storage format", err)
	}
}

func TestParseConfigRejectsInvalidJournalFormat(t *testing.T) {
	_, err := parseConfig([]string{
		"-monitoring-server",
		"-journal-format", "msgpack",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "unsupported command journal format") {
		t.Fatalf("parseConfig(invalid journal format) error = %v, want unsupported command journal format", err)
	}
}

func TestParseConfigRejectsPartialMonitoringTLSConfig(t *testing.T) {
	if _, err := parseConfig([]string{"-monitoring-tls-cert", "/tmp/cert.pem"}, &bytes.Buffer{}); err == nil {
		t.Fatal("parseConfig(partial TLS) error = nil, want error")
	}
	if _, err := parseConfig([]string{"-monitoring-tls-key", "/tmp/key.pem"}, &bytes.Buffer{}); err == nil {
		t.Fatal("parseConfig(partial TLS key) error = nil, want error")
	}
}

func TestParseConfigRejectsPartialGRPCTLSConfig(t *testing.T) {
	if _, err := parseConfig([]string{"-grpc-tls-cert", "/tmp/cert.pem"}, &bytes.Buffer{}); err == nil {
		t.Fatal("parseConfig(partial gRPC TLS cert) error = nil, want error")
	}
	if _, err := parseConfig([]string{"-grpc-tls-key", "/tmp/key.pem"}, &bytes.Buffer{}); err == nil {
		t.Fatal("parseConfig(partial gRPC TLS key) error = nil, want error")
	}
	if _, err := parseConfig([]string{"-grpc-client-ca", "/tmp/client-ca.pem"}, &bytes.Buffer{}); err == nil {
		t.Fatal("parseConfig(gRPC client CA without TLS) error = nil, want error")
	}
}

func TestMonitoringTLSConfigEnablesHTTP2(t *testing.T) {
	base := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{
			"acme-tls/1",
		},
	}
	cfg := monitoringTLSConfig(base)
	if cfg == base {
		t.Fatal("monitoringTLSConfig returned base config, want clone")
	}
	if !containsString(cfg.NextProtos, "h2") {
		t.Fatalf("NextProtos = %#v, want h2", cfg.NextProtos)
	}
	if !containsString(cfg.NextProtos, "http/1.1") {
		t.Fatalf("NextProtos = %#v, want http/1.1", cfg.NextProtos)
	}
	if !containsString(cfg.NextProtos, "acme-tls/1") {
		t.Fatalf("NextProtos = %#v, want preserved custom protocol", cfg.NextProtos)
	}
	if base.NextProtos[0] != "acme-tls/1" || len(base.NextProtos) != 1 {
		t.Fatalf("base NextProtos mutated: %#v", base.NextProtos)
	}
}

func TestMonitoringServerServesHTTP2OverTLS(t *testing.T) {
	certPath, keyPath := writeTestCertificate(t)

	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen error = %v", err)
	}

	handler := hatriecache.NewMonitoringHandler(ht, hatriecache.MonitoringOptions{}).Handler()
	server := &http.Server{
		Handler:   handler,
		TLSConfig: monitoringTLSConfig(nil),
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeTLS(listener, certPath, keyPath)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Fatalf("server shutdown error = %v", err)
		}
		if err := <-errCh; err != nil && err != http.ErrServerClosed {
			t.Fatalf("server error = %v", err)
		}
	})

	transport := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport}

	resp, err := client.Get("https://" + listener.Addr().String() + "/api/health")
	if err != nil {
		t.Fatalf("GET /api/health error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if resp.ProtoMajor != 2 {
		t.Fatalf("protocol = %s, want HTTP/2", resp.Proto)
	}
}

func TestStopGRPCServerWithTimeoutHandlesNilAndFastStop(t *testing.T) {
	stopGRPCServerWithTimeout(nil, time.Millisecond)

	server := grpc.NewServer()
	stopped := make(chan struct{})
	go func() {
		stopGRPCServerWithTimeout(server, time.Hour)
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("stopGRPCServerWithTimeout(fast) did not return")
	}
}

func TestStopGRPCServerWithTimeoutForcesBlockedRPC(t *testing.T) {
	entered := make(chan struct{})
	released := make(chan struct{})
	interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if info.FullMethod != "/hatriecache.v1.CacheService/Health" {
			return handler(ctx, req)
		}
		close(entered)
		<-ctx.Done()
		close(released)
		return nil, ctx.Err()
	}

	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	hatriecache.RegisterCacheGRPCServer(server, hatriecache.NewCacheGRPCServer(ht, hatriecache.CacheGRPCOptions{}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := grpc.DialContext(ctx, listener.Addr().String(),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	cancel()
	if err != nil {
		server.Stop()
		t.Fatalf("DialContext() error = %v", err)
	}
	defer conn.Close()

	client := hatriecachev1.NewCacheServiceClient(conn)
	rpcErr := make(chan error, 1)
	go func() {
		_, err := client.Health(context.Background(), &hatriecachev1.HealthRequest{})
		rpcErr <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		server.Stop()
		t.Fatal("blocked health RPC did not enter interceptor")
	}

	stopped := make(chan struct{})
	go func() {
		stopGRPCServerWithTimeout(server, 10*time.Millisecond)
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		server.Stop()
		t.Fatal("stopGRPCServerWithTimeout(timeout) did not force stop")
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("blocked health RPC was not released")
	}
	if err := <-rpcErr; err == nil {
		t.Fatal("blocked health RPC error = nil, want cancellation error")
	}
	if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatalf("Serve() error = %v, want nil or ErrServerStopped", err)
	}
}

func TestRunDoesNotStartServerByDefault(t *testing.T) {
	stdout := &bytes.Buffer{}
	if err := run(nil, nil, stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !strings.Contains(stdout.String(), "monitoring server disabled") {
		t.Fatalf("stdout = %q, want disabled message", stdout.String())
	}
}

func TestRunCheckConfigDoesNotStartListeners(t *testing.T) {
	monitoringAddr := freeTCPAddr(t)
	grpcAddr := freeTCPAddr(t)
	stdout := &bytes.Buffer{}

	if err := run(context.Background(), []string{
		"-check-config",
		"-monitoring-server",
		"-monitoring-addr", monitoringAddr,
		"-grpc-addr", grpcAddr,
	}, stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("run(check-config) error = %v", err)
	}
	if !strings.Contains(stdout.String(), "configuration ok") {
		t.Fatalf("stdout = %q, want configuration ok", stdout.String())
	}
	assertAddrAvailable(t, monitoringAddr)
	assertAddrAvailable(t, grpcAddr)
}

func TestRunCheckConfigValidatesTLSAndTopology(t *testing.T) {
	serverCertPath, serverKeyPath := writeTestCertificate(t)
	clientCertPath, _ := writeTestClientCertificate(t)
	topologyPath := filepath.Join(t.TempDir(), "topology.json")
	if err := hatriecache.SaveTopology(topologyPath, hatriecache.SingleNodeTopology("node-a", "http://127.0.0.1:8080")); err != nil {
		t.Fatalf("SaveTopology() error = %v", err)
	}

	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-check-config",
		"-monitoring-tls-cert", serverCertPath,
		"-monitoring-tls-key", serverKeyPath,
		"-grpc-tls-cert", serverCertPath,
		"-grpc-tls-key", serverKeyPath,
		"-grpc-client-ca", clientCertPath,
		"-topology-path", topologyPath,
	}, stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("run(check-config valid references) error = %v", err)
	}
	if !strings.Contains(stdout.String(), "configuration ok") {
		t.Fatalf("stdout = %q, want configuration ok", stdout.String())
	}
}

func TestRunCheckConfigRejectsInvalidReferences(t *testing.T) {
	serverCertPath, serverKeyPath := writeTestCertificate(t)
	dir := t.TempDir()
	invalidCAPath := filepath.Join(dir, "client-ca.pem")
	if err := os.WriteFile(invalidCAPath, []byte("not pem"), 0o600); err != nil {
		t.Fatalf("WriteFile(client CA) error = %v", err)
	}
	invalidTopologyPath := filepath.Join(dir, "topology.json")
	if err := os.WriteFile(invalidTopologyPath, []byte(`{"nodes":`), 0o600); err != nil {
		t.Fatalf("WriteFile(topology) error = %v", err)
	}

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "client CA",
			args: []string{
				"-check-config",
				"-grpc-tls-cert", serverCertPath,
				"-grpc-tls-key", serverKeyPath,
				"-grpc-client-ca", invalidCAPath,
			},
			want: "gRPC client CA",
		},
		{
			name: "topology",
			args: []string{
				"-check-config",
				"-topology-path", invalidTopologyPath,
			},
			want: "load topology",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(context.Background(), tt.args, &bytes.Buffer{}, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("run(check-config %s) error = %v, want %q", tt.name, err, tt.want)
			}
		})
	}
}

func TestRunPrintConfigRedactsSecretsAndDoesNotStartListeners(t *testing.T) {
	monitoringAddr := freeTCPAddr(t)
	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-print-config",
		"-monitoring-server",
		"-monitoring-addr", monitoringAddr,
		"-monitoring-auth-token", "super-secret",
		"-replication-auth-token", "replica-secret",
		"-rate-limit", "4",
	}, stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("run(print-config) error = %v", err)
	}
	body := stdout.String()
	if strings.Contains(body, "super-secret") || strings.Contains(body, "replica-secret") {
		t.Fatalf("print-config leaked auth token: %s", body)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("print-config JSON error = %v\n%s", err, body)
	}
	if got["monitoring_auth_token"] != "<redacted>" || got["replication_auth_token"] != "<redacted>" || got["monitoring_addr"] != monitoringAddr || got["rate_limit"] != float64(4) {
		t.Fatalf("print-config = %#v, want redacted tokens, address, and rate limit", got)
	}
	assertAddrAvailable(t, monitoringAddr)
}

func TestRunCheckConfigCanPrintValidatedConfig(t *testing.T) {
	certPath, keyPath := writeTestCertificate(t)
	stdout := &bytes.Buffer{}
	if err := run(context.Background(), []string{
		"-check-config",
		"-print-config",
		"-monitoring-tls-cert", certPath,
		"-monitoring-tls-key", keyPath,
	}, stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("run(check-config print-config) error = %v", err)
	}
	if strings.Contains(stdout.String(), "configuration ok") {
		t.Fatalf("stdout = %q, want JSON config only", stdout.String())
	}
	var got map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("check-config print-config JSON error = %v\n%s", err, stdout.String())
	}
	if got["monitoring_tls_cert"] != certPath {
		t.Fatalf("printed config monitoring_tls_cert = %v, want %s", got["monitoring_tls_cert"], certPath)
	}
}

func TestRunRejectsJournalPullWithoutJournalPath(t *testing.T) {
	stdout := &bytes.Buffer{}
	err := run(context.Background(), []string{
		"-monitoring-server",
		"-monitoring-addr", freeTCPAddr(t),
		"-journal-pull-source", "http://leader:8080",
	}, stdout, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "journal pull requires -journal-path") {
		t.Fatalf("run() error = %v, want journal path requirement", err)
	}
	if strings.Contains(stdout.String(), "monitoring server listening") {
		t.Fatalf("stdout = %q, want no monitoring startup message", stdout.String())
	}
}

func TestRunJournalPullDefaultsStatePath(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := hatriecache.CommandJournalTail{
			LastSequence: 1,
			Entries: []hatriecache.CommandJournalRecord{
				{Sequence: 1, Request: hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	defer source.Close()

	dir := t.TempDir()
	journalPath := filepath.Join(dir, "commands.journal")
	statePath := journalPath + ".pull_state.json"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"-monitoring-server",
			"-monitoring-addr", freeTCPAddr(t),
			"-journal-path", journalPath,
			"-journal-pull-source", source.URL,
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	waitUntil(t, 5*time.Second, func() bool {
		after, err := loadJournalPullState(statePath, source.URL)
		return err == nil && after == 1
	})
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() error = %v, want clean shutdown", err)
		}
	case <-time.After(serverShutdownTimeout + time.Second):
		t.Fatal("run() did not stop after context cancel")
	}
}

func TestRunDoesNotStartMonitoringWhenGRPCBindFails(t *testing.T) {
	monitoringAddr := freeTCPAddr(t)
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("blocking listen error = %v", err)
	}
	defer blocker.Close()

	stdout := &bytes.Buffer{}
	err = run(context.Background(), []string{
		"-monitoring-server",
		"-monitoring-addr", monitoringAddr,
		"-grpc-addr", blocker.Addr().String(),
	}, stdout, &bytes.Buffer{})
	if err == nil {
		t.Fatal("run() error = nil, want gRPC bind error")
	}
	if strings.Contains(stdout.String(), "monitoring server listening") {
		t.Fatalf("stdout = %q, want no monitoring startup message", stdout.String())
	}

	listener, err := net.Listen("tcp", monitoringAddr)
	if err != nil {
		t.Fatalf("monitoring address %s remained in use after failed startup: %v", monitoringAddr, err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("monitoring listener Close() error = %v", err)
	}
}

func TestRunStartsHTTPAndGRPCAndStopsOnContextCancel(t *testing.T) {
	monitoringAddr := freeTCPAddr(t)
	grpcAddr := freeTCPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"-monitoring-server",
			"-monitoring-addr", monitoringAddr,
			"-monitoring-web-dir", "",
			"-grpc-addr", grpcAddr,
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	waitForHTTPHealth(t, "http://"+monitoringAddr+"/api/health")
	waitForGRPCHealth(t, grpcAddr)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() after cancel error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
	waitForAddrReusable(t, monitoringAddr)
	waitForAddrReusable(t, grpcAddr)
}

func TestRunServesRedactedConfigEndpoint(t *testing.T) {
	monitoringAddr := freeTCPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"-monitoring-server",
			"-monitoring-addr", monitoringAddr,
			"-monitoring-web-dir", "",
			"-monitoring-auth-token", "super-secret",
			"-replication-auth-token", "replica-secret",
			"-rate-limit", "9",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	baseURL := "http://" + monitoringAddr
	waitForHTTPHealthWithToken(t, baseURL+"/api/health", "super-secret")
	req, err := http.NewRequest(http.MethodGet, baseURL+"/api/config", nil)
	if err != nil {
		t.Fatalf("NewRequest(/api/config) error = %v", err)
	}
	req.Header.Set("Authorization", "Bearer super-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/config error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/api/config status = %d, want 200", resp.StatusCode)
	}
	var got map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("/api/config JSON error = %v", err)
	}
	if got["monitoring_auth_token"] != "<redacted>" || got["replication_auth_token"] != "<redacted>" || got["rate_limit"] != float64(9) || got["monitoring_addr"] != monitoringAddr {
		t.Fatalf("/api/config = %#v, want redacted tokens, rate limit, and address", got)
	}
	if text := fmt.Sprint(got); strings.Contains(text, "super-secret") || strings.Contains(text, "replica-secret") {
		t.Fatalf("/api/config leaked auth token: %s", text)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() after cancel error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
	waitForAddrReusable(t, monitoringAddr)
}

func TestRunWritesAuditLog(t *testing.T) {
	monitoringAddr := freeTCPAddr(t)
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"-monitoring-server",
			"-monitoring-addr", monitoringAddr,
			"-monitoring-web-dir", "",
			"-audit-log-path", auditPath,
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	waitForHTTPHealth(t, "http://"+monitoringAddr+"/api/health")
	resp, err := http.Post("http://"+monitoringAddr+"/api/commands", "application/json", strings.NewReader(`{"command":"SETSTR","key":"name","value":"ivi"}`))
	if err != nil {
		t.Fatalf("POST /api/commands error = %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("command status = %d, want 200", resp.StatusCode)
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() after cancel error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}

	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("ReadFile(audit log) error = %v", err)
	}
	text := string(data)
	if !strings.Contains(text, `"action":"command"`) || !strings.Contains(text, `"key":"name"`) {
		t.Fatalf("audit log = %s, want command event", text)
	}
	if strings.Contains(text, "ivi") {
		t.Fatalf("audit log leaked command value: %s", text)
	}
}

func TestNewGRPCServerPassesMonitoringAuthToken(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	server, listener, err := newGRPCServer(config{
		grpcAddr:            "127.0.0.1:0",
		monitoringAuthToken: "secret",
	}, ht, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("newGRPCServer() error = %v", err)
	}
	defer listener.Close()
	defer server.Stop()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := grpc.DialContext(ctx, listener.Addr().String(),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	cancel()
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	defer conn.Close()

	client := hatriecachev1.NewCacheServiceClient(conn)
	_, err = client.Health(context.Background(), &hatriecachev1.HealthRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Health(unauthenticated) error = %v, want Unauthenticated", err)
	}

	authCtx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer secret")
	if _, err := client.Health(authCtx, &hatriecachev1.HealthRequest{}); err != nil {
		t.Fatalf("Health(authenticated) error = %v", err)
	}

	server.Stop()
	if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatalf("gRPC Serve() error = %v", err)
	}
}

func TestNewGRPCServerUsesTLS(t *testing.T) {
	certPath, keyPath := writeTestCertificate(t)
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	server, listener, err := newGRPCServer(config{
		grpcAddr:    "127.0.0.1:0",
		grpcTLSCert: certPath,
		grpcTLSKey:  keyPath,
	}, ht, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("newGRPCServer(TLS) error = %v", err)
	}
	defer listener.Close()
	defer server.Stop()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := grpc.DialContext(ctx, listener.Addr().String(),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})),
	)
	cancel()
	if err != nil {
		t.Fatalf("DialContext(TLS) error = %v", err)
	}
	defer conn.Close()

	client := hatriecachev1.NewCacheServiceClient(conn)
	if _, err := client.Health(context.Background(), &hatriecachev1.HealthRequest{}); err != nil {
		t.Fatalf("Health(TLS) error = %v", err)
	}

	server.Stop()
	if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatalf("gRPC Serve() error = %v", err)
	}
}

func TestNewGRPCServerRequiresClientCertificate(t *testing.T) {
	serverCertPath, serverKeyPath := writeTestCertificate(t)
	clientCertPath, clientKeyPath := writeTestClientCertificate(t)
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	server, listener, err := newGRPCServer(config{
		grpcAddr:     "127.0.0.1:0",
		grpcTLSCert:  serverCertPath,
		grpcTLSKey:   serverKeyPath,
		grpcClientCA: clientCertPath,
	}, ht, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("newGRPCServer(mTLS) error = %v", err)
	}
	defer listener.Close()
	defer server.Stop()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	connWithoutClientCert, err := grpc.DialContext(ctx, listener.Addr().String(),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})),
	)
	cancel()
	if err == nil {
		client := hatriecachev1.NewCacheServiceClient(connWithoutClientCert)
		_, healthErr := client.Health(context.Background(), &hatriecachev1.HealthRequest{})
		_ = connWithoutClientCert.Close()
		if healthErr == nil {
			t.Fatal("Health(mTLS without client certificate) error = nil, want rejection")
		}
	}

	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		t.Fatalf("LoadX509KeyPair(client) error = %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	connWithClientCert, err := grpc.DialContext(ctx, listener.Addr().String(),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
			Certificates:       []tls.Certificate{clientCert},
		})),
	)
	cancel()
	if err != nil {
		t.Fatalf("DialContext(mTLS client cert) error = %v", err)
	}
	defer connWithClientCert.Close()

	client := hatriecachev1.NewCacheServiceClient(connWithClientCert)
	if _, err := client.Health(context.Background(), &hatriecachev1.HealthRequest{}); err != nil {
		t.Fatalf("Health(mTLS client cert) error = %v", err)
	}

	server.Stop()
	if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatalf("gRPC Serve() error = %v", err)
	}
}

func TestRunRefreshesLocalElectionHeartbeat(t *testing.T) {
	monitoringAddr := freeTCPAddr(t)
	topologyPath := filepath.Join(t.TempDir(), "topology.json")
	if err := hatriecache.SaveTopology(topologyPath, hatriecache.ClusterTopology{
		Self: "node-a",
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://" + monitoringAddr},
			{ID: "node-b", Address: "http://127.0.0.1:1"},
		},
		Shards: []hatriecache.TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	}); err != nil {
		t.Fatalf("SaveTopology() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"-monitoring-server",
			"-monitoring-addr", monitoringAddr,
			"-monitoring-web-dir", "",
			"-node-id", "node-a",
			"-topology-path", topologyPath,
			"-election-timeout", "80ms",
			"-enforce-leader-writes",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	waitForHTTPHealth(t, "http://"+monitoringAddr+"/api/health")
	time.Sleep(240 * time.Millisecond)
	resp, err := http.Post("http://"+monitoringAddr+"/api/commands", "application/json", strings.NewReader(`{"command":"SETSTR","key":"session:1","value":"value"}`))
	if err != nil {
		t.Fatalf("POST /api/commands error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var body bytes.Buffer
		_, _ = body.ReadFrom(resp.Body)
		t.Fatalf("leader write status = %d body %q, want 200", resp.StatusCode, body.String())
	}
	var response hatriecache.CacheCommandResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("command response JSON error = %v", err)
	}
	if !response.OK {
		t.Fatalf("command response = %#v, want ok after local heartbeat refresh", response)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() after cancel error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
	waitForAddrReusable(t, monitoringAddr)
}

func TestRunReplicatesBetweenTwoMonitoringServers(t *testing.T) {
	addrA := freeTCPAddr(t)
	addrB := freeTCPAddr(t)
	dir := t.TempDir()
	topologyAPath, topologyBPath := writeTwoNodeRunTopologies(t, dir, addrA, addrB)

	ctxB, cancelB := context.WithCancel(context.Background())
	errB := make(chan error, 1)
	go func() {
		errB <- run(ctxB, []string{
			"-monitoring-server",
			"-monitoring-addr", addrB,
			"-monitoring-web-dir", "",
			"-node-id", "node-b",
			"-topology-path", topologyBPath,
			"-enforce-leader-writes",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	defer stopRunServerForTest(t, cancelB, errB, addrB)

	ctxA, cancelA := context.WithCancel(context.Background())
	errA := make(chan error, 1)
	go func() {
		errA <- run(ctxA, []string{
			"-monitoring-server",
			"-monitoring-addr", addrA,
			"-monitoring-web-dir", "",
			"-node-id", "node-a",
			"-topology-path", topologyAPath,
			"-replication",
			"-replication-mode", "command",
			"-enforce-leader-writes",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	defer stopRunServerForTest(t, cancelA, errA, addrA)

	waitForHTTPHealth(t, "http://"+addrA+"/api/health")
	waitForHTTPHealth(t, "http://"+addrB+"/api/health")

	write := postTestCommand(t, "http://"+addrA, `{"command":"SETSTR","key":"session:multi","value":"from-a"}`)
	if !write.OK {
		t.Fatalf("node-a write response = %#v, want ok", write)
	}
	waitUntil(t, 5*time.Second, func() bool {
		read := postTestCommand(t, "http://"+addrB, `{"command":"GETSTR","key":"session:multi"}`)
		return read.OK && read.Value == "from-a"
	})

	resp, err := http.Get("http://" + addrA + "/api/replication")
	if err != nil {
		t.Fatalf("GET node-a replication status error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("node-a replication status = %d, want 200", resp.StatusCode)
	}
	var result hatriecache.ReplicationResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Decode(replication status) error = %v", err)
	}
	if result.Skipped || len(result.Targets) != 1 || result.Targets[0].Node != "node-b" || !result.Targets[0].OK {
		t.Fatalf("replication result = %#v, want successful node-b target", result)
	}
}

func TestRunReplicationAuthFailureReportsUnauthorizedTarget(t *testing.T) {
	addrA := freeTCPAddr(t)
	addrB := freeTCPAddr(t)
	dir := t.TempDir()
	topologyAPath, topologyBPath := writeTwoNodeRunTopologies(t, dir, addrA, addrB)

	ctxB, cancelB := context.WithCancel(context.Background())
	errB := make(chan error, 1)
	go func() {
		errB <- run(ctxB, []string{
			"-monitoring-server",
			"-monitoring-addr", addrB,
			"-monitoring-web-dir", "",
			"-node-id", "node-b",
			"-topology-path", topologyBPath,
			"-replication-auth-token", "replica-secret",
			"-enforce-leader-writes",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	defer stopRunServerForTest(t, cancelB, errB, addrB)

	ctxA, cancelA := context.WithCancel(context.Background())
	errA := make(chan error, 1)
	go func() {
		errA <- run(ctxA, []string{
			"-monitoring-server",
			"-monitoring-addr", addrA,
			"-monitoring-web-dir", "",
			"-node-id", "node-a",
			"-topology-path", topologyAPath,
			"-replication",
			"-replication-mode", "command",
			"-replication-auth-token", "wrong-secret",
			"-enforce-leader-writes",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	defer stopRunServerForTest(t, cancelA, errA, addrA)

	waitForHTTPHealth(t, "http://"+addrA+"/api/health")
	waitForHTTPHealth(t, "http://"+addrB+"/api/health")

	write := postTestCommand(t, "http://"+addrA, `{"command":"SETSTR","key":"session:auth","value":"blocked"}`)
	if !write.OK {
		t.Fatalf("node-a write response = %#v, want local ok", write)
	}
	result := waitForReplicationResult(t, "http://"+addrA, func(result hatriecache.ReplicationResult) bool {
		return len(result.Targets) == 1 && result.Targets[0].Node == "node-b" && !result.Targets[0].OK && result.Targets[0].Status == http.StatusUnauthorized
	})
	if result.Health != "degraded" && result.Health != "unhealthy" {
		t.Fatalf("replication health = %q for %#v, want degraded or unhealthy", result.Health, result)
	}
	if got := postTestCommand(t, "http://"+addrB, `{"command":"EXISTS","key":"session:auth"}`); !got.OK || got.Value != "0" {
		t.Fatalf("node-b EXISTS after unauthorized replication = %#v, want missing key", got)
	}
}

func TestRunAsyncLevelDBOutboxReplaysAfterTargetRestart(t *testing.T) {
	addrA := freeTCPAddr(t)
	addrB := freeTCPAddr(t)
	dir := t.TempDir()
	topologyAPath, topologyBPath := writeTwoNodeRunTopologies(t, dir, addrA, addrB)
	outboxPath := filepath.Join(dir, "replication-outbox.leveldb")

	nodeAArgs := []string{
		"-monitoring-server",
		"-monitoring-addr", addrA,
		"-monitoring-web-dir", "",
		"-node-id", "node-a",
		"-topology-path", topologyAPath,
		"-replication",
		"-replication-mode", "command",
		"-replication-async",
		"-replication-queue-size", "8",
		"-replication-retry-interval", "5s",
		"-replication-max-attempts", "10",
		"-replication-outbox-path", outboxPath,
		"-replication-outbox-format", "leveldb",
		"-enforce-leader-writes",
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	errA := make(chan error, 1)
	go func() {
		errA <- run(ctxA, nodeAArgs, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	waitForHTTPHealth(t, "http://"+addrA+"/api/health")

	write := postTestCommand(t, "http://"+addrA, `{"command":"SETSTR","key":"session:outbox","value":"after-restart"}`)
	if !write.OK {
		t.Fatalf("node-a async write response = %#v, want local ok", write)
	}
	stopRunServerForTest(t, cancelA, errA, addrA)

	ctxB, cancelB := context.WithCancel(context.Background())
	errB := make(chan error, 1)
	go func() {
		errB <- run(ctxB, []string{
			"-monitoring-server",
			"-monitoring-addr", addrB,
			"-monitoring-web-dir", "",
			"-node-id", "node-b",
			"-topology-path", topologyBPath,
			"-enforce-leader-writes",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	defer stopRunServerForTest(t, cancelB, errB, addrB)
	waitForHTTPHealth(t, "http://"+addrB+"/api/health")

	ctxA2, cancelA2 := context.WithCancel(context.Background())
	errA2 := make(chan error, 1)
	go func() {
		errA2 <- run(ctxA2, nodeAArgs, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	defer stopRunServerForTest(t, cancelA2, errA2, addrA)
	waitForHTTPHealth(t, "http://"+addrA+"/api/health")

	waitUntil(t, 5*time.Second, func() bool {
		read := postTestCommand(t, "http://"+addrB, `{"command":"GETSTR","key":"session:outbox"}`)
		return read.OK && read.Value == "after-restart"
	})
	result := waitForReplicationResult(t, "http://"+addrA, func(result hatriecache.ReplicationResult) bool {
		return result.Command == "SETSTR" && result.Key == "session:outbox" && len(result.Targets) == 1 && result.Targets[0].OK
	})
	if result.Queue == nil || result.Queue.Depth != 0 {
		t.Fatalf("node-a replication queue = %#v, want empty queue after replay", result.Queue)
	}
}

func TestRunReplicaAcceptsLeaderWriteAfterPrimaryMarkedOffline(t *testing.T) {
	addrA := freeTCPAddr(t)
	addrB := freeTCPAddr(t)
	dir := t.TempDir()
	_, topologyBPath := writeTwoNodeRunTopologies(t, dir, addrA, addrB)

	ctxB, cancelB := context.WithCancel(context.Background())
	errB := make(chan error, 1)
	go func() {
		errB <- run(ctxB, []string{
			"-monitoring-server",
			"-monitoring-addr", addrB,
			"-monitoring-web-dir", "",
			"-node-id", "node-b",
			"-topology-path", topologyBPath,
			"-enforce-leader-writes",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	defer stopRunServerForTest(t, cancelB, errB, addrB)
	waitForHTTPHealth(t, "http://"+addrB+"/api/health")

	status, before := postTestCommandStatus(t, "http://"+addrB, `{"command":"SETSTR","key":"session:failover","value":"before"}`)
	if status != http.StatusConflict || before.OK {
		t.Fatalf("node-b pre-failover write status/response = %d/%#v, want 409 leader rejection", status, before)
	}

	postTestElectionUpdate(t, "http://"+addrB, `{"node":"node-a","online":false}`)
	after := postTestCommand(t, "http://"+addrB, `{"command":"SETSTR","key":"session:failover","value":"after"}`)
	if !after.OK {
		t.Fatalf("node-b post-failover write response = %#v, want ok", after)
	}
	if got := postTestCommand(t, "http://"+addrB, `{"command":"GETSTR","key":"session:failover"}`); !got.OK || got.Value != "after" {
		t.Fatalf("node-b post-failover GETSTR = %#v, want after", got)
	}
}

func TestRunRejectsReplicationWithStaleTopologyFingerprint(t *testing.T) {
	addrA := freeTCPAddr(t)
	addrB := freeTCPAddr(t)
	dir := t.TempDir()
	topologyAPath := writeTwoNodeRunTopology(t, dir, "node-a-topology.json", "node-a", addrA, addrB, 1)
	topologyBPath := writeTwoNodeRunTopologyWithRoles(t, dir, "node-b-topology.json", "node-b", addrA, addrB, 1, "", "replica")

	ctxB, cancelB := context.WithCancel(context.Background())
	errB := make(chan error, 1)
	go func() {
		errB <- run(ctxB, []string{
			"-monitoring-server",
			"-monitoring-addr", addrB,
			"-monitoring-web-dir", "",
			"-node-id", "node-b",
			"-topology-path", topologyBPath,
			"-enforce-leader-writes",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	defer stopRunServerForTest(t, cancelB, errB, addrB)

	ctxA, cancelA := context.WithCancel(context.Background())
	errA := make(chan error, 1)
	go func() {
		errA <- run(ctxA, []string{
			"-monitoring-server",
			"-monitoring-addr", addrA,
			"-monitoring-web-dir", "",
			"-node-id", "node-a",
			"-topology-path", topologyAPath,
			"-replication",
			"-replication-mode", "command",
			"-enforce-leader-writes",
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	defer stopRunServerForTest(t, cancelA, errA, addrA)

	waitForHTTPHealth(t, "http://"+addrA+"/api/health")
	waitForHTTPHealth(t, "http://"+addrB+"/api/health")

	write := postTestCommand(t, "http://"+addrA, `{"command":"SETSTR","key":"session:stale-topology","value":"blocked"}`)
	if !write.OK {
		t.Fatalf("node-a stale-topology write response = %#v, want local ok", write)
	}
	waitForReplicationResult(t, "http://"+addrA, func(result hatriecache.ReplicationResult) bool {
		return len(result.Targets) == 1 && result.Targets[0].Node == "node-b" && !result.Targets[0].OK && result.Targets[0].Status == http.StatusConflict
	})
	if got := postTestCommand(t, "http://"+addrB, `{"command":"EXISTS","key":"session:stale-topology"}`); !got.OK || got.Value != "0" {
		t.Fatalf("node-b EXISTS after stale topology replication = %#v, want missing key", got)
	}
}

func TestStartElectionHeartbeatStopIsIdempotent(t *testing.T) {
	topology, err := hatriecache.NewTopologyStore(hatriecache.SingleNodeTopology("node-a", "http://127.0.0.1"))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	election := hatriecache.NewElectionStore(topology, hatriecache.ElectionOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop, err := startElectionHeartbeat(ctx, election, "node-a", time.Hour, true, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("startElectionHeartbeat() error = %v", err)
	}
	cancel()

	stopped := make(chan struct{})
	go func() {
		stop()
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("election heartbeat repeated stop did not return")
	}
}

func TestReportServerErrorDoesNotBlockWhenChannelIsFull(t *testing.T) {
	errCh := make(chan error, 1)
	errCh <- errors.New("first")

	done := make(chan struct{})
	go func() {
		reportServerError(errCh, errors.New("second"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("reportServerError blocked on a full channel")
	}
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free TCP listen error = %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("free TCP listener Close() error = %v", err)
	}
	return addr
}

func writeTwoNodeRunTopologies(t *testing.T, dir string, addrA string, addrB string) (string, string) {
	t.Helper()
	return writeTwoNodeRunTopology(t, dir, "node-a-topology.json", "node-a", addrA, addrB, 0),
		writeTwoNodeRunTopology(t, dir, "node-b-topology.json", "node-b", addrA, addrB, 0)
}

func writeTwoNodeRunTopology(t *testing.T, dir string, filename string, self string, addrA string, addrB string, version uint64) string {
	t.Helper()
	return writeTwoNodeRunTopologyWithRoles(t, dir, filename, self, addrA, addrB, version, "", "")
}

func writeTwoNodeRunTopologyWithRoles(t *testing.T, dir string, filename string, self string, addrA string, addrB string, version uint64, nodeARole string, nodeBRole string) string {
	t.Helper()
	topologyAPath := filepath.Join(dir, "node-a-topology.json")
	if filename != "" {
		topologyAPath = filepath.Join(dir, filename)
	}
	topology := hatriecache.ClusterTopology{
		Version: version,
		Self:    self,
		Nodes: []hatriecache.TopologyNode{
			{ID: "node-a", Address: "http://" + addrA, Role: nodeARole},
			{ID: "node-b", Address: "http://" + addrB, Role: nodeBRole},
		},
		Shards: []hatriecache.TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	}
	if err := hatriecache.SaveTopology(topologyAPath, topology); err != nil {
		t.Fatalf("SaveTopology(%s) error = %v", self, err)
	}
	return topologyAPath
}

func waitForHTTPHealth(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("HTTP health endpoint %s did not become ready", url)
}

func waitForReplicationResult(t *testing.T, baseURL string, accept func(hatriecache.ReplicationResult) bool) hatriecache.ReplicationResult {
	t.Helper()
	var last hatriecache.ReplicationResult
	waitUntil(t, 5*time.Second, func() bool {
		resp, err := http.Get(baseURL + "/api/replication")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var result hatriecache.ReplicationResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return false
		}
		last = result
		return accept(result)
	})
	return last
}

func waitForHTTPHealthWithToken(t *testing.T, url string, token string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("NewRequest(%s) error = %v", url, err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("HTTP health endpoint %s did not become ready", url)
}

func postTestCommand(t *testing.T, baseURL string, body string) hatriecache.CacheCommandResponse {
	t.Helper()
	status, response := postTestCommandStatus(t, baseURL, body)
	if status != http.StatusOK {
		t.Fatalf("POST %s/api/commands status = %d response %#v, want 200", baseURL, status, response)
	}
	return response
}

func postTestCommandStatus(t *testing.T, baseURL string, body string) (int, hatriecache.CacheCommandResponse) {
	t.Helper()
	resp, err := http.Post(baseURL+"/api/commands", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s/api/commands error = %v", baseURL, err)
	}
	defer resp.Body.Close()
	var response hatriecache.CacheCommandResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("Decode(%s/api/commands) error = %v", baseURL, err)
	}
	return resp.StatusCode, response
}

func postTestElectionUpdate(t *testing.T, baseURL string, body string) {
	t.Helper()
	resp, err := http.Post(baseURL+"/api/election", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s/api/election error = %v", baseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var text bytes.Buffer
		_, _ = text.ReadFrom(resp.Body)
		t.Fatalf("POST %s/api/election status = %d body %q, want 200", baseURL, resp.StatusCode, text.String())
	}
}

func stopRunServerForTest(t *testing.T, cancel context.CancelFunc, errCh <-chan error, addr string) {
	t.Helper()
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run(%s) after cancel error = %v", addr, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("run(%s) did not return after context cancellation", addr)
	}
	waitForAddrReusable(t, addr)
}

func waitForGRPCHealth(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		cancel()
		if err == nil {
			client := hatriecachev1.NewCacheServiceClient(conn)
			callCtx, callCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_, callErr := client.Health(callCtx, &hatriecachev1.HealthRequest{})
			callCancel()
			if closeErr := conn.Close(); closeErr != nil {
				t.Fatalf("gRPC connection Close() error = %v", closeErr)
			}
			if callErr == nil {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("gRPC health endpoint %s did not become ready", addr)
}

func waitForAddrReusable(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		listener, err := net.Listen("tcp", addr)
		if err == nil {
			if err := listener.Close(); err != nil {
				t.Fatalf("listener Close() error = %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("address %s was not released", addr)
}

func assertAddrAvailable(t *testing.T, addr string) {
	t.Helper()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("address %s is not available: %v", addr, err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("listener Close() error = %v", err)
	}
}

func TestLoadSnapshotIfConfiguredIgnoresMissingSnapshot(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	if _, err := loadSnapshotIfConfigured(ht, filepath.Join(t.TempDir(), "missing.json")); err != nil {
		t.Fatalf("loadSnapshotIfConfigured(missing) error = %v", err)
	}
}

func TestSnapshotLifecycleHelpersLoadAndSave(t *testing.T) {
	source := hatriecache.CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("key", "value")

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := source.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := hatriecache.CreateHatTrie()
	defer loaded.Destroy()
	if _, err := loadSnapshotIfConfigured(loaded, path); err != nil {
		t.Fatalf("loadSnapshotIfConfigured() error = %v", err)
	}
	if got := loaded.GetString("key"); got != "value" {
		t.Fatalf("loaded key = %q, want value", got)
	}

	loaded.UpsertString("second", "value2")
	if err := loaded.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot(second) error = %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Fatalf("snapshot file info = %v/%v, want non-empty file", info, err)
	}
}

func TestLevelDBLifecycleHelpersLoadAndSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := hatriecache.CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("key", "value")

	store, err := openLevelDBIfConfigured(path, hatriecache.DefaultStorageFormat)
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})
	if err := store.Save(source); err != nil {
		t.Fatalf("store.Save() error = %v", err)
	}

	loaded := hatriecache.CreateHatTrie()
	defer loaded.Destroy()
	if err := loadLevelDBIfConfigured(loaded, store, hatriecache.LevelDBLoadPolicy{}); err != nil {
		t.Fatalf("loadLevelDBIfConfigured() error = %v", err)
	}
	if got := loaded.GetString("key"); got != "value" {
		t.Fatalf("loaded key = %q, want value", got)
	}
}

func TestRunDoesNotSaveLevelDBWhenRestoreFails(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cache.leveldb")
	dbSource := hatriecache.CreateHatTrie()
	defer dbSource.Destroy()
	dbSource.UpsertString("key", "leveldb")
	if err := dbSource.SaveLevelDB(dbPath); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	snapshotDir := t.TempDir()
	snapshotPath := filepath.Join(snapshotDir, "snapshot.json")
	snapshotSource := hatriecache.CreateHatTrie()
	defer snapshotSource.Destroy()
	snapshotSource.UpsertString("key", "snapshot")
	if err := snapshotSource.SaveSnapshot(snapshotPath); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if err := os.Chmod(snapshotDir, 0o500); err != nil {
		t.Fatalf("Chmod(snapshot dir) error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(snapshotDir, 0o700)
	})
	probePath := filepath.Join(snapshotDir, "probe")
	if err := os.WriteFile(probePath, []byte("probe"), 0o600); err == nil {
		_ = os.Remove(probePath)
		t.Skip("snapshot directory remains writable after chmod")
	}

	err := run(context.Background(), []string{
		"-monitoring-server",
		"-db-path", dbPath,
		"-snapshot-path", snapshotPath,
		"-journal-path", filepath.Join(t.TempDir(), "commands.journal"),
	}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("run() error = nil, want snapshot save error")
	}

	loaded := hatriecache.CreateHatTrie()
	defer loaded.Destroy()
	if _, err := loaded.LoadLevelDB(dbPath); err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if got := loaded.GetString("key"); got != "leveldb" {
		t.Fatalf("LevelDB key after failed restore = %q, want original leveldb value", got)
	}
}

func TestTopologyLifecycleHelperLoadsAndSaves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topology.json")
	store, err := openTopologyIfConfigured(path, "node-a", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("openTopologyIfConfigured() error = %v", err)
	}
	if got := store.Get(); got.Self != "node-a" {
		t.Fatalf("fallback topology = %#v, want node-a", got)
	}

	updated := hatriecache.SingleNodeTopology("node-b", "127.0.0.1:8081")
	if err := store.Set(updated); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	reloaded, err := openTopologyIfConfigured(path, "node-a", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("openTopologyIfConfigured(reload) error = %v", err)
	}
	if got := reloaded.Get(); got.Self != "node-b" {
		t.Fatalf("reloaded topology = %#v, want node-b", got)
	}
}

func writeTestCertificate(t *testing.T) (string, string) {
	return writeTestCertificateWithExtKeyUsage(t, "localhost", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
}

func writeTestClientCertificate(t *testing.T) (string, string) {
	return writeTestCertificateWithExtKeyUsage(t, "hatrie-client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
}

func writeTestCertificateWithExtKeyUsage(t *testing.T, commonName string, usages []x509.ExtKeyUsage) (string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey error = %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: usages,
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate error = %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	certFile, err := os.OpenFile(certPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("open cert file error = %v", err)
	}
	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		certFile.Close()
		t.Fatalf("write cert error = %v", err)
	}
	if err := certFile.Close(); err != nil {
		t.Fatalf("close cert file error = %v", err)
	}

	keyFile, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("open key file error = %v", err)
	}
	if err := pem.Encode(keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}); err != nil {
		keyFile.Close()
		t.Fatalf("write key error = %v", err)
	}
	if err := keyFile.Close(); err != nil {
		t.Fatalf("close key file error = %v", err)
	}

	return certPath, keyPath
}

func TestStartSnapshotSaverWritesPeriodically(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	stop := startSnapshotSaver(ctx, ht, nil, path, time.Millisecond, hatriecache.SnapshotFormatJSON, &bytes.Buffer{})
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("periodic snapshot was not written")
}

func TestStartSnapshotSaverWritesImmediately(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	stop := startSnapshotSaver(ctx, ht, nil, path, time.Hour, hatriecache.SnapshotFormatJSON, &bytes.Buffer{})
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("initial snapshot was not written")
}

func TestStartSnapshotSaverStopIsIdempotent(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	ctx, cancel := context.WithCancel(context.Background())
	path := filepath.Join(t.TempDir(), "snapshot.json")
	stop := startSnapshotSaver(ctx, ht, nil, path, time.Hour, hatriecache.SnapshotFormatJSON, &bytes.Buffer{})
	cancel()

	assertStopReturns(t, stop, "snapshot saver repeated stop")
}

func TestSaveSnapshotIfConfiguredReturnsDestroyedTrieError(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	ht.Destroy()

	err := saveSnapshotIfConfigured(ht, nil, filepath.Join(t.TempDir(), "snapshot.json"), hatriecache.SnapshotFormatJSON)
	if !errors.Is(err, errHatTrieDestroyed) {
		t.Fatalf("saveSnapshotIfConfigured() error = %v, want destroyed trie error", err)
	}
}

func TestStartSnapshotSaverStopsAfterDestroy(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	stop := startSnapshotSaver(context.Background(), ht, nil, filepath.Join(t.TempDir(), "snapshot.json"), time.Millisecond, hatriecache.SnapshotFormatJSON, &bytes.Buffer{})
	ht.Destroy()
	time.Sleep(20 * time.Millisecond)

	assertStopReturns(t, stop, "snapshot saver stop after destroy")
}

func TestStartLevelDBSaverWritesPeriodically(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	store, err := openLevelDBIfConfigured(filepath.Join(t.TempDir(), "cache.leveldb"), hatriecache.DefaultStorageFormat)
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startLevelDBSaver(ctx, ht, store, nil, time.Millisecond, &bytes.Buffer{})
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entry, ok, err := store.Entry("key")
		if err == nil && ok && entry.Type == "string" && entry.String == "value" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("periodic LevelDB save was not written")
}

func TestStartLevelDBSaverWritesImmediately(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	store, err := openLevelDBIfConfigured(filepath.Join(t.TempDir(), "cache.leveldb"), hatriecache.DefaultStorageFormat)
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startLevelDBSaver(ctx, ht, store, nil, time.Hour, &bytes.Buffer{})
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entry, ok, err := store.Entry("key")
		if err == nil && ok && entry.Type == "string" && entry.String == "value" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("initial LevelDB save was not written")
}

func TestStartLevelDBSaverWritesDirtyKeysAfterInitialSave(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "old")

	store, err := openLevelDBIfConfigured(filepath.Join(t.TempDir(), "cache.leveldb"), hatriecache.DefaultStorageFormat)
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})
	dirty := hatriecache.NewLevelDBDirtyTracker()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startLevelDBSaver(ctx, ht, store, dirty, time.Millisecond, &bytes.Buffer{})
	defer stop()

	waitUntil(t, 2*time.Second, func() bool {
		entry, ok, err := store.Entry("key")
		return err == nil && ok && entry.Type == "string" && entry.String == "old"
	})
	ht.UpsertString("key", "new")
	dirty.Mark("key")
	waitUntil(t, 2*time.Second, func() bool {
		entry, ok, err := store.Entry("key")
		return err == nil && ok && entry.Type == "string" && entry.String == "new" && dirty.Pending() == 0
	})
}

func TestStartLevelDBCompactorRunsPeriodically(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("alpha", "one")
	ht.UpsertString("omega", "two")

	store, err := openLevelDBIfConfigured(filepath.Join(t.TempDir(), "cache.leveldb"), hatriecache.DefaultStorageFormat)
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})
	if err := store.Save(ht); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	results := make(chan hatriecache.LevelDBCompactionResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startLevelDBCompactor(ctx, store, time.Millisecond, levelDBCompactorOptions{
		StartKey: "alpha",
		LimitKey: "omega\x00",
	}, func(result hatriecache.LevelDBCompactionResult) {
		select {
		case results <- result:
		default:
		}
	}, &bytes.Buffer{})
	defer stop()

	select {
	case result := <-results:
		if result.Store != "leveldb" || result.StartKey != "alpha" || result.LimitKey != "omega\x00" || result.DurationMillis < 0 {
			t.Fatalf("compaction result = %#v, want leveldb range result", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("periodic LevelDB compaction did not run")
	}
}

func TestStartLevelDBSaverStopIsIdempotent(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	store, err := openLevelDBIfConfigured(filepath.Join(t.TempDir(), "cache.leveldb"), hatriecache.DefaultStorageFormat)
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})

	ctx, cancel := context.WithCancel(context.Background())
	stop := startLevelDBSaver(ctx, ht, store, nil, time.Hour, &bytes.Buffer{})
	cancel()

	assertStopReturns(t, stop, "LevelDB saver repeated stop")
}

func TestSaveLevelDBIfOpenReturnsDestroyedTrieError(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	store, err := openLevelDBIfConfigured(filepath.Join(t.TempDir(), "cache.leveldb"), hatriecache.DefaultStorageFormat)
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})

	ht.Destroy()
	err = saveLevelDBIfOpen(ht, store)
	if !errors.Is(err, errHatTrieDestroyed) {
		t.Fatalf("saveLevelDBIfOpen() error = %v, want destroyed trie error", err)
	}
}

func TestStartLevelDBSaverStopsAfterDestroy(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	store, err := openLevelDBIfConfigured(filepath.Join(t.TempDir(), "cache.leveldb"), hatriecache.DefaultStorageFormat)
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})

	stop := startLevelDBSaver(context.Background(), ht, store, nil, time.Millisecond, &bytes.Buffer{})
	ht.Destroy()
	time.Sleep(20 * time.Millisecond)

	assertStopReturns(t, stop, "LevelDB saver stop after destroy")
}

func TestPeriodicHelpersAcceptNilContext(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	snapshotPath := filepath.Join(t.TempDir(), "snapshot.json")
	stopSnapshot := startSnapshotSaver(nil, ht, nil, snapshotPath, time.Hour, hatriecache.SnapshotFormatJSON, &bytes.Buffer{})
	waitUntil(t, 2*time.Second, func() bool {
		info, err := os.Stat(snapshotPath)
		return err == nil && info.Size() > 0
	})
	stopSnapshot()

	store, err := openLevelDBIfConfigured(filepath.Join(t.TempDir(), "cache.leveldb"), hatriecache.DefaultStorageFormat)
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})
	stopLevelDB := startLevelDBSaver(nil, ht, store, nil, time.Hour, &bytes.Buffer{})
	waitUntil(t, 2*time.Second, func() bool {
		entry, ok, err := store.Entry("key")
		return err == nil && ok && entry.Type == "string" && entry.String == "value"
	})
	stopLevelDB()

	topology, err := hatriecache.NewTopologyStore(hatriecache.SingleNodeTopology("node-a", "http://127.0.0.1:1"))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	election := hatriecache.NewElectionStore(topology, hatriecache.ElectionOptions{})
	stopElection, err := startElectionHeartbeat(nil, election, "node-a", time.Hour, true, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("startElectionHeartbeat(nil context) error = %v", err)
	}
	stopElection()

	replicator := hatriecache.NewHTTPReplicator(hatriecache.HTTPReplicatorOptions{})
	stopReplication := startReplicationSyncer(nil, ht, replicator, time.Hour, "", &bytes.Buffer{})
	stopReplication()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := hatriecache.CommandJournalTail{
			LastSequence: 1,
			Entries: []hatriecache.CommandJournalRecord{
				{Sequence: 1, Request: hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "pulled", Value: "ok"}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	defer source.Close()
	journal, err := hatriecache.OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	stopJournal := startJournalPuller(nil, ht, journal, journalPullerConfig{
		Source:     source.URL,
		StatePath:  filepath.Join(t.TempDir(), "pull-state.json"),
		Limit:      10,
		MaxBatches: 1,
	}, &bytes.Buffer{})
	waitUntil(t, 2*time.Second, func() bool {
		return ht.GetString("pulled") == "ok"
	})
	stopJournal()
}

func TestSnapshotCallbackRequiresPath(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	if got := snapshotCallback(ht, nil, "", hatriecache.SnapshotFormatJSON); got != nil {
		t.Fatal("snapshotCallback(empty) returned callback, want nil")
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	callback := snapshotCallback(ht, nil, path, hatriecache.SnapshotFormatJSON)
	if callback == nil {
		t.Fatal("snapshotCallback(path) = nil, want callback")
	}
	if err := callback(); err != nil {
		t.Fatalf("snapshot callback error = %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Fatalf("snapshot file info = %v/%v, want non-empty file", info, err)
	}
}

func TestJournalPullerAppliesAndPersistsState(t *testing.T) {
	requests := make(chan string, 1)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.String()
		if r.URL.Query().Get("after_sequence") != "" {
			t.Fatalf("after_sequence = %q, want empty first pull", r.URL.Query().Get("after_sequence"))
		}
		response := hatriecache.CommandJournalTail{
			LastSequence: 1,
			Entries: []hatriecache.CommandJournalRecord{
				{Sequence: 1, Request: hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	defer source.Close()

	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	journal, err := hatriecache.OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	statePath := filepath.Join(t.TempDir(), "pull-state.json")

	stop := startJournalPuller(context.Background(), ht, journal, journalPullerConfig{
		Source:     source.URL,
		StatePath:  statePath,
		Limit:      10,
		MaxBatches: 5,
	}, &bytes.Buffer{})
	waitUntil(t, 5*time.Second, func() bool {
		return ht.GetString("name") == "ivi"
	})
	stop()

	select {
	case path := <-requests:
		if path != "/api/journal?limit=10" {
			t.Fatalf("source path = %q, want /api/journal?limit=10", path)
		}
	default:
		t.Fatal("journal puller did not call source")
	}
	after, err := loadJournalPullState(statePath, source.URL)
	if err != nil {
		t.Fatalf("loadJournalPullState() error = %v", err)
	}
	if after != 1 {
		t.Fatalf("pull state after = %d, want 1", after)
	}
}

func TestPullJournalOncePersistsPartialProgressOnApplyError(t *testing.T) {
	badTTL := int64(-1)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := hatriecache.CommandJournalTail{
			LastSequence: 2,
			Entries: []hatriecache.CommandJournalRecord{
				{Sequence: 1, Request: hatriecache.CacheCommandRequest{Command: "INC", Key: "views", Value: "1"}},
				{Sequence: 2, Request: hatriecache.CacheCommandRequest{Command: "SETSTR", Key: "bad", Value: "value", TTLSeconds: &badTTL}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	defer source.Close()

	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	journal, err := hatriecache.OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	statePath := filepath.Join(t.TempDir(), "pull-state.json")

	result, err := pullJournalOnce(context.Background(), ht, journal, journalPullerConfig{
		Source:     source.URL,
		StatePath:  statePath,
		Limit:      10,
		MaxBatches: 1,
	})
	if err == nil {
		t.Fatal("pullJournalOnce() error = nil, want apply error")
	}
	if result.Applied != 1 || result.AppliedThrough != 1 {
		t.Fatalf("pull result = %#v, want one applied entry through sequence 1", result)
	}
	if got := ht.GetCounter("views"); got != 1 {
		t.Fatalf("views after partial pull = %d, want 1", got)
	}
	after, err := loadJournalPullState(statePath, source.URL)
	if err != nil {
		t.Fatalf("loadJournalPullState() error = %v", err)
	}
	if after != 1 {
		t.Fatalf("pull state after partial error = %d, want 1", after)
	}
}

func TestJournalPullerStopCancelsInFlightPull(t *testing.T) {
	entered := make(chan struct{})
	released := make(chan struct{})
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(released)
	}))
	defer source.Close()

	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	journal, err := hatriecache.OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	stop := startJournalPuller(context.Background(), ht, journal, journalPullerConfig{
		Source:     source.URL,
		Limit:      10,
		MaxBatches: 1,
	}, &bytes.Buffer{})

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("journal pull source was not called")
	}

	stopped := make(chan struct{})
	go func() {
		stop()
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("journal puller repeated stop did not cancel in-flight pull")
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("journal pull source did not observe request cancellation")
	}
}

func TestJournalPullStateRejectsSourceMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pull-state.json")
	if err := saveJournalPullState(path, "http://leader-a", 7); err != nil {
		t.Fatalf("saveJournalPullState() error = %v", err)
	}
	if _, err := loadJournalPullState(path, "http://leader-b"); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("loadJournalPullState(mismatch) error = %v, want source mismatch", err)
	}
}

func TestDecodeJournalPullStateJSONReaderStreamsState(t *testing.T) {
	state, err := decodeJournalPullStateJSONReader(iotest.OneByteReader(strings.NewReader(`{"source":"http://leader-a","applied_through":7}`)))
	if err != nil {
		t.Fatalf("decodeJournalPullStateJSONReader() error = %v", err)
	}
	if state.Source != "http://leader-a" || state.AppliedThrough != 7 {
		t.Fatalf("decoded state = %#v, want leader-a through sequence 7", state)
	}
}

func TestDecodeJournalPullStateJSONReaderRejectsInvalidEnvelope(t *testing.T) {
	for name, payload := range map[string]string{
		"unknown":  `{"source":"http://leader-a","applied_through":7,"unexpected":true}`,
		"trailing": `{"source":"http://leader-a","applied_through":7} trailing`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeJournalPullStateJSONReader(strings.NewReader(payload)); err == nil {
				t.Fatal("decodeJournalPullStateJSONReader() error = nil, want rejection")
			}
		})
	}
}

func TestWriteJSONFileAtomicReplacesFileAndCleansTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	if err := writeJSONFileAtomic(path, map[string]interface{}{"sequence": 1}); err != nil {
		t.Fatalf("writeJSONFileAtomic(first) error = %v", err)
	}
	if err := writeJSONFileAtomic(path, map[string]interface{}{"sequence": 2}); err != nil {
		t.Fatalf("writeJSONFileAtomic(second) error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(data), `"sequence": 2`) {
		t.Fatalf("file payload = %q, want second JSON payload", data)
	}
	assertNoJSONAtomicTempFiles(t, dir, "state.json")
}

func TestWriteJSONFileAtomicCleansTemporaryFileOnEncodeError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	if err := writeJSONFileAtomic(path, map[string]interface{}{"sequence": 1}); err != nil {
		t.Fatalf("writeJSONFileAtomic(previous) error = %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(before) error = %v", err)
	}
	if err := writeJSONFileAtomic(path, map[string]interface{}{"bad": make(chan int)}); err == nil {
		t.Fatal("writeJSONFileAtomic(unsupported value) error = nil, want error")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(after) error = %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("file after failed write = %q, want previous %q", after, before)
	}
	assertNoJSONAtomicTempFiles(t, dir, "state.json")
}

func TestWriteJSONFileAtomicCleansTemporaryFileOnRenameError(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "state.json")
	if err := os.Mkdir(targetDir, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	if err := writeJSONFileAtomic(targetDir, map[string]interface{}{"sequence": 1}); err == nil {
		t.Fatal("writeJSONFileAtomic(directory target) error = nil, want error")
	}
	assertNoJSONAtomicTempFiles(t, dir, "state.json")
	if info, err := os.Stat(targetDir); err != nil || !info.IsDir() {
		t.Fatalf("target directory = %v/%v, want existing directory", info, err)
	}
}

func TestJournaledSnapshotHelpersCheckpointAndCompact(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.json")
	journalPath := filepath.Join(dir, "commands.journal")

	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	journal, err := openJournalIfConfigured(journalPath, hatriecache.DefaultCommandJournalFormat)
	if err != nil {
		t.Fatalf("openJournalIfConfigured() error = %v", err)
	}
	if got := journal.ExecuteCommand(ht, hatriecache.CacheCommandRequest{Command: "SETINT", Key: "views", Value: "1"}); !got.OK {
		t.Fatalf("journaled SETINT response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, hatriecache.CacheCommandRequest{Command: "INC", Key: "views", Value: "2"}); !got.OK {
		t.Fatalf("journaled INC response = %#v, want ok", got)
	}

	if err := saveSnapshotIfConfigured(ht, journal, snapshotPath, hatriecache.SnapshotFormatJSON); err != nil {
		t.Fatalf("saveSnapshotIfConfigured() error = %v", err)
	}
	if info, err := os.Stat(journalPath); err != nil || info.Size() == 0 {
		t.Fatalf("journal file after compact = %v/%v, want checkpoint marker", info, err)
	}

	loaded := hatriecache.CreateHatTrie()
	defer loaded.Destroy()
	metadata, err := loadSnapshotIfConfigured(loaded, snapshotPath)
	if err != nil {
		t.Fatalf("loadSnapshotIfConfigured() error = %v", err)
	}
	if metadata.JournalSequence != 2 {
		t.Fatalf("journal sequence = %d, want 2", metadata.JournalSequence)
	}
	if got := loaded.GetCounter("views"); got != 3 {
		t.Fatalf("loaded views = %d, want 3", got)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}

func assertStopReturns(t *testing.T, stop func(), label string) {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		stop()
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		t.Fatal(label + " did not return")
	}
}

func assertNoJSONAtomicTempFiles(t *testing.T, dir string, base string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s) error = %v", dir, err)
	}
	prefix := "." + base + ".tmp-"
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			t.Fatalf("temporary file %q was not cleaned up", entry.Name())
		}
	}
}
