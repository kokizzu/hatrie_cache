package hatriecache

import (
	"os"
	"strings"
	"testing"
)

func TestMonitoringWrapperPassesExplicitFormatDefaults(t *testing.T) {
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	script, err := os.ReadFile("scripts/monitoring-server.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/monitoring-server.sh) error = %v", err)
	}

	makefileText := string(makefile)
	for _, token := range []string{
		"REPLICATION_WIRE_FORMAT ?= protobuf",
		"DB_FORMAT ?= binary",
		"SNAPSHOT_FORMAT ?= gzip-best-binary",
		"JOURNAL_FORMAT ?= binary",
	} {
		if !strings.Contains(makefileText, token) {
			t.Fatalf("Makefile missing explicit format default %q", token)
		}
	}

	scriptText := string(script)
	for _, token := range []string{
		"replication_wire_format=${REPLICATION_WIRE_FORMAT:-protobuf}",
		"db_format=${DB_FORMAT:-binary}",
		"snapshot_format=${SNAPSHOT_FORMAT:-gzip-best-binary}",
		"journal_format=${JOURNAL_FORMAT:-binary}",
		`set -- "$@" -replication-wire-format "$replication_wire_format"`,
		`set -- "$@" -db-format "$db_format"`,
		`set -- "$@" -snapshot-format "$snapshot_format"`,
		`set -- "$@" -journal-format "$journal_format"`,
	} {
		if !strings.Contains(scriptText, token) {
			t.Fatalf("monitoring wrapper missing explicit format default token %q", token)
		}
	}
}

func TestMonitoringWrapperPassesAuthToken(t *testing.T) {
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	if !strings.Contains(string(makefile), "MONITORING_AUTH_TOKEN ?=\n") || !strings.Contains(string(makefile), "MONITORING_AUTH_TOKEN='$(MONITORING_AUTH_TOKEN)'") {
		t.Fatal("Makefile should expose MONITORING_AUTH_TOKEN to the monitoring wrapper")
	}

	script, err := os.ReadFile("scripts/monitoring-server.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/monitoring-server.sh) error = %v", err)
	}
	scriptText := string(script)
	for _, token := range []string{
		"auth_token=${MONITORING_AUTH_TOKEN:-}",
		`-monitoring-auth-token "$auth_token"`,
	} {
		if !strings.Contains(scriptText, token) {
			t.Fatalf("monitoring wrapper missing auth token plumbing %q", token)
		}
	}
}

func TestMonitoringWrapperPassesAuditLogPath(t *testing.T) {
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	if !strings.Contains(string(makefile), "AUDIT_LOG_PATH ?=\n") || !strings.Contains(string(makefile), "AUDIT_LOG_PATH='$(AUDIT_LOG_PATH)'") {
		t.Fatal("Makefile should expose AUDIT_LOG_PATH to the monitoring wrapper")
	}

	script, err := os.ReadFile("scripts/monitoring-server.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/monitoring-server.sh) error = %v", err)
	}
	scriptText := string(script)
	for _, token := range []string{
		"audit_log_path=${AUDIT_LOG_PATH:-}",
		`-audit-log-path "$audit_log_path"`,
	} {
		if !strings.Contains(scriptText, token) {
			t.Fatalf("monitoring wrapper missing audit log plumbing %q", token)
		}
	}
}

func TestMonitoringWrapperPassesWriteProtectionAndRateLimit(t *testing.T) {
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefileText := string(makefile)
	for _, token := range []string{
		"WRITE_PROTECTION ?= false",
		"RATE_LIMIT ?= 0",
		"WRITE_PROTECTION='$(WRITE_PROTECTION)'",
		"RATE_LIMIT='$(RATE_LIMIT)'",
	} {
		if !strings.Contains(makefileText, token) {
			t.Fatalf("Makefile missing write protection/rate limit token %q", token)
		}
	}

	script, err := os.ReadFile("scripts/monitoring-server.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/monitoring-server.sh) error = %v", err)
	}
	scriptText := string(script)
	for _, token := range []string{
		"write_protection=${WRITE_PROTECTION:-false}",
		"rate_limit=${RATE_LIMIT:-0}",
		`-write-protection="$write_protection"`,
		`-rate-limit "$rate_limit"`,
	} {
		if !strings.Contains(scriptText, token) {
			t.Fatalf("monitoring wrapper missing write protection/rate limit token %q", token)
		}
	}
}

func TestMonitoringWrapperPassesMemoryGovernorOptions(t *testing.T) {
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefileText := string(makefile)
	for _, token := range []string{
		"DB_MEMORY_CAP_BYTES ?= 0",
		"DB_RSS_CAP_BYTES ?= 0",
		"DB_MEMORY_EVICT_INTERVAL ?= 0",
		"DB_MEMORY_EVICT_MIN_VALUE_BYTES ?= 1024",
		"DB_MEMORY_CAP_BYTES='$(DB_MEMORY_CAP_BYTES)'",
		"DB_RSS_CAP_BYTES='$(DB_RSS_CAP_BYTES)'",
		"DB_MEMORY_EVICT_INTERVAL='$(DB_MEMORY_EVICT_INTERVAL)'",
		"DB_MEMORY_EVICT_MIN_VALUE_BYTES='$(DB_MEMORY_EVICT_MIN_VALUE_BYTES)'",
	} {
		if !strings.Contains(makefileText, token) {
			t.Fatalf("Makefile missing memory governor token %q", token)
		}
	}

	script, err := os.ReadFile("scripts/monitoring-server.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/monitoring-server.sh) error = %v", err)
	}
	scriptText := string(script)
	for _, token := range []string{
		"db_memory_cap_bytes=${DB_MEMORY_CAP_BYTES:-0}",
		"db_rss_cap_bytes=${DB_RSS_CAP_BYTES:-0}",
		"db_memory_evict_interval=${DB_MEMORY_EVICT_INTERVAL:-0}",
		"db_memory_evict_min_value_bytes=${DB_MEMORY_EVICT_MIN_VALUE_BYTES:-1024}",
		`-db-memory-cap-bytes "$db_memory_cap_bytes"`,
		`-db-rss-cap-bytes "$db_rss_cap_bytes"`,
		`-db-memory-evict-interval "$db_memory_evict_interval"`,
		`-db-memory-evict-min-value-bytes "$db_memory_evict_min_value_bytes"`,
	} {
		if !strings.Contains(scriptText, token) {
			t.Fatalf("monitoring wrapper missing memory governor token %q", token)
		}
	}
}

func TestStorageWrappersUseCLI(t *testing.T) {
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefileText := string(makefile)
	for _, token := range []string{
		"STORAGE_PEER ?= http://127.0.0.1:8080",
		"storage-status:",
		"storage-flush:",
		"storage-compact:",
		"STORAGE_PEER='$(STORAGE_PEER)' ./scripts/storage-flush.sh",
	} {
		if !strings.Contains(makefileText, token) {
			t.Fatalf("Makefile missing storage wrapper token %q", token)
		}
	}

	for path, tokens := range map[string][]string{
		"scripts/storage-status.sh":  {"storage status"},
		"scripts/storage-flush.sh":   {"storage flush"},
		"scripts/storage-compact.sh": {"storage compact", "STORAGE_COMPACT_START_KEY", "STORAGE_COMPACT_LIMIT_KEY"},
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		text := string(data)
		for _, token := range tokens {
			if !strings.Contains(text, token) {
				t.Fatalf("%s missing token %q", path, token)
			}
		}
	}
}

func TestServerWrapperPassesConfigFile(t *testing.T) {
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefileText := string(makefile)
	for _, token := range []string{
		"CONFIG_PATH ?=\n",
		"SERVER_ARGS ?=\n",
		"server:",
		"CONFIG_PATH='$(CONFIG_PATH)' ./scripts/server.sh $(SERVER_ARGS)",
	} {
		if !strings.Contains(makefileText, token) {
			t.Fatalf("Makefile missing server config token %q", token)
		}
	}

	script, err := os.ReadFile("scripts/server.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/server.sh) error = %v", err)
	}
	scriptText := string(script)
	for _, token := range []string{
		"config_path=${CONFIG_PATH:-}",
		`set -- -config "$config_path" "$@"`,
		"exec go run ./cmd/hatrie-cache",
	} {
		if !strings.Contains(scriptText, token) {
			t.Fatalf("server wrapper missing config token %q", token)
		}
	}
}

func TestClusterStatusWrapperWiring(t *testing.T) {
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefileText := string(makefile)
	for _, token := range []string{
		"CLUSTER_PEER ?= http://127.0.0.1:8080",
		"CLUSTER_PROBE_NODES ?= true",
		"cluster-status:",
		"./scripts/cluster-status.sh",
	} {
		if !strings.Contains(makefileText, token) {
			t.Fatalf("Makefile missing cluster status token %q", token)
		}
	}

	script, err := os.ReadFile("scripts/cluster-status.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/cluster-status.sh) error = %v", err)
	}
	scriptText := string(script)
	for _, token := range []string{
		"peer=${CLUSTER_PEER:-http://127.0.0.1:8080}",
		"probe_nodes=${CLUSTER_PROBE_NODES:-true}",
		"cluster status -peer",
		"-probe-nodes=false",
	} {
		if !strings.Contains(scriptText, token) {
			t.Fatalf("cluster status wrapper missing token %q", token)
		}
	}
}

func TestVerifyCScriptUsesRunScopedTemporaryDirectory(t *testing.T) {
	data, err := os.ReadFile("scripts/verify-c.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/verify-c.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		`mktemp -d "${TMPDIR:-/tmp}/hatrie_cache_c_verify.XXXXXX"`,
		`trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM`,
		`"$tmp_dir/check_hattrie"`,
		`"$tmp_dir/check_ahtable"`,
		`"$tmp_dir/check_hattrie_sanitize"`,
		`"$tmp_dir/check_ahtable_sanitize"`,
		`"$tmp_dir/check_hattrie_leak"`,
		`"$tmp_dir/check_ahtable_leak"`,
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("verify-c script should use run-scoped temporary paths; missing %q", token)
		}
	}
	for _, stalePath := range []string{
		"/tmp/hatrie_cache_check_hattrie",
		"/tmp/hatrie_cache_check_ahtable",
		"/tmp/hatrie_cache_check_hattrie_sanitize",
		"/tmp/hatrie_cache_check_ahtable_sanitize",
		"/tmp/hatrie_cache_check_hattrie_leak",
		"/tmp/hatrie_cache_check_ahtable_leak",
		"/tmp/c_asan_probe.",
		"/tmp/c_lsan_probe.",
	} {
		if strings.Contains(script, stalePath) {
			t.Fatalf("verify-c script still uses fixed temporary path %q", stalePath)
		}
	}
}

func TestVerifyCScriptRunsLeakSanitizerFallbackWhenAddressSanitizerIsSkipped(t *testing.T) {
	data, err := os.ReadFile("scripts/verify-c.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/verify-c.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"compiler_supports_leak_sanitizer()",
		"run_leak_sanitizer=1",
		`LEAK_FLAGS="-fsanitize=leak -fno-omit-frame-pointer"`,
		`LSAN_OPTIONS=${LSAN_OPTIONS:-detect_leaks=1:halt_on_error=1:exitcode=23}`,
		"running C leak sanitizer fallback",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("verify-c script should include LeakSanitizer fallback; missing %q", token)
		}
	}
}

func TestVerifyOpsScriptExercisesRestoreAndJournalPull(t *testing.T) {
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefileText := string(makefile)
	for _, token := range []string{
		"verify: verify-go verify-c verify-frontend verify-ops",
		"verify-ops:",
		"./scripts/verify-ops.sh",
	} {
		if !strings.Contains(makefileText, token) {
			t.Fatalf("Makefile should wire verify-ops into verify; missing %q", token)
		}
	}

	data, err := os.ReadFile("scripts/verify-ops.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/verify-ops.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"run_snapshot_journal_restore_smoke",
		"run_journal_pull_smoke",
		"-snapshot-path",
		"-journal-path",
		"-journal-pull-source",
		"journal -pull-from",
		"expect_value",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("verify-ops script should exercise operational workflow token %q", token)
		}
	}
}
