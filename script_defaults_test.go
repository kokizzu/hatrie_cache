package hatriecache

import (
	"os"
	"strings"
	"testing"
)

func TestMonitoringWrapperDefersFormatDefaultsToGoConfig(t *testing.T) {
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
		"REPLICATION_WIRE_FORMAT ?=\n",
		"DB_FORMAT ?=\n",
		"SNAPSHOT_FORMAT ?=\n",
		"JOURNAL_FORMAT ?=\n",
	} {
		if !strings.Contains(makefileText, token) {
			t.Fatalf("Makefile should leave format override empty by default; missing %q", token)
		}
	}

	scriptText := string(script)
	for _, token := range []string{
		"replication_wire_format=${REPLICATION_WIRE_FORMAT:-}",
		"db_format=${DB_FORMAT:-}",
		"snapshot_format=${SNAPSHOT_FORMAT:-}",
		"journal_format=${JOURNAL_FORMAT:-}",
		`set -- "$@" -replication-wire-format "$replication_wire_format"`,
		`set -- "$@" -db-format "$db_format"`,
		`set -- "$@" -snapshot-format "$snapshot_format"`,
		`set -- "$@" -journal-format "$journal_format"`,
	} {
		if !strings.Contains(scriptText, token) {
			t.Fatalf("monitoring wrapper should pass explicit format overrides only; missing %q", token)
		}
	}
	for _, staleDefault := range []string{
		"REPLICATION_WIRE_FORMAT:-protobuf",
		"DB_FORMAT:-binary",
		"SNAPSHOT_FORMAT:-gzip-best-binary",
		"JOURNAL_FORMAT:-binary",
	} {
		if strings.Contains(scriptText, staleDefault) {
			t.Fatalf("monitoring wrapper still duplicates Go default %q", staleDefault)
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
