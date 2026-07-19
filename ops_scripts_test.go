package hatriecache

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBackupAndRestoreScriptsCopyDataDirectory(t *testing.T) {
	tempDir := t.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	backupDir := filepath.Join(tempDir, "backup", "run-001")
	restoreDir := filepath.Join(tempDir, "restore")

	writeTestFile(t, filepath.Join(dataDir, "snapshot.hc"), "snapshot")
	writeTestFile(t, filepath.Join(dataDir, "commands.journal"), "journal")
	writeTestFile(t, filepath.Join(dataDir, "cache.leveldb", "000001.ldb"), "leveldb")

	runScript(t, "scripts/backup.sh",
		"DATA_DIR="+dataDir,
		"BACKUP_DIR="+backupDir,
	)

	for _, path := range []string{
		filepath.Join(backupDir, "snapshot.hc"),
		filepath.Join(backupDir, "commands.journal"),
		filepath.Join(backupDir, "cache.leveldb", "000001.ldb"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("backup missing %s: %v", path, err)
		}
	}

	runScript(t, "scripts/restore.sh",
		"DATA_DIR="+restoreDir,
		"BACKUP_DIR="+backupDir,
	)

	for _, path := range []string{
		filepath.Join(restoreDir, "snapshot.hc"),
		filepath.Join(restoreDir, "commands.journal"),
		filepath.Join(restoreDir, "cache.leveldb", "000001.ldb"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("restore missing %s: %v", path, err)
		}
	}
}

func TestRestoreScriptRejectsNonEmptyDataDirectoryByDefault(t *testing.T) {
	tempDir := t.TempDir()
	backupDir := filepath.Join(tempDir, "backup")
	restoreDir := filepath.Join(tempDir, "restore")
	writeTestFile(t, filepath.Join(backupDir, "snapshot.hc"), "snapshot")
	writeTestFile(t, filepath.Join(restoreDir, "existing"), "keep")

	output, err := runScriptOutput("scripts/restore.sh",
		"DATA_DIR="+restoreDir,
		"BACKUP_DIR="+backupDir,
	)
	if err == nil {
		t.Fatal("restore into non-empty DATA_DIR succeeded, want rejection")
	}
	if !strings.Contains(output, "DATA_DIR is not empty") {
		t.Fatalf("restore rejection output = %q, want non-empty DATA_DIR message", output)
	}
}

func TestBackupScriptRejectsBackupInsideDataDirectory(t *testing.T) {
	tempDir := t.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	writeTestFile(t, filepath.Join(dataDir, "snapshot.hc"), "snapshot")

	output, err := runScriptOutput("scripts/backup.sh",
		"DATA_DIR="+dataDir,
		"BACKUP_DIR="+filepath.Join(dataDir, "backup"),
	)
	if err == nil {
		t.Fatal("backup inside DATA_DIR succeeded, want rejection")
	}
	if !strings.Contains(output, "BACKUP_DIR must not be inside DATA_DIR") {
		t.Fatalf("backup rejection output = %q, want recursive backup message", output)
	}
}

func TestMakefileWiresBackupRestoreTargets(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefile := string(data)
	for _, token := range []string{
		"DATA_DIR ?= data",
		"BACKUP_DIR ?= backup/latest",
		"backup:",
		"./scripts/backup.sh",
		"restore:",
		"./scripts/restore.sh",
		"restore-bundle:",
		"./scripts/restore-bundle.sh",
		"restore-rehearsal:",
		"./scripts/restore-rehearsal.sh",
		"RESTORE_REHEARSAL_RUNTIME_CHECK ?= true",
		"RESTORE_REHEARSAL_RUNTIME_GET ?=",
		"RESTORE_REHEARSAL_RUNTIME_SERVER_BIN ?=",
		"doctor:",
		"./scripts/doctor.sh",
		"check-config:",
		"./scripts/check-config.sh",
		"CONFIG_PROFILE ?= production",
		"print-sane-config:",
		"./scripts/print-sane-config.sh",
		"bench-ci-smoke:",
		"./scripts/benchmark-ci-smoke.sh",
		"bench-journal-catchup:",
		"./scripts/benchmark-journal-catchup.sh",
		"GRPC_TLS_CERT ?=",
		"GRPC_CLIENT_CA ?=",
	} {
		if !strings.Contains(makefile, token) {
			t.Fatalf("Makefile missing backup/restore token %q", token)
		}
	}

	data, err = os.ReadFile("scripts/doctor.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/doctor.sh) error = %v", err)
	}
	if !strings.Contains(string(data), "go run ./cmd/hatrie-cli doctor -path") {
		t.Fatal("doctor script should invoke hatrie-cli doctor")
	}
	data, err = os.ReadFile("scripts/check-config.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/check-config.sh) error = %v", err)
	}
	for _, token := range []string{
		"-check-config",
		"CONFIG_PATH",
		"go run ./cmd/hatrie-cache",
	} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("check-config script missing token %q", token)
		}
	}
	data, err = os.ReadFile("scripts/print-sane-config.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/print-sane-config.sh) error = %v", err)
	}
	for _, token := range []string{
		"CONFIG_PROFILE",
		"-print-config",
		"-profile",
		"go run ./cmd/hatrie-cache",
	} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("print-sane-config script missing token %q", token)
		}
	}
	data, err = os.ReadFile("scripts/monitoring-server.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/monitoring-server.sh) error = %v", err)
	}
	for _, token := range []string{
		"GRPC_TLS_CERT",
		"-grpc-tls-cert",
		"GRPC_CLIENT_CA",
		"-grpc-client-ca",
		"REPLICATION_DEAD_LETTER_LIMIT",
		"-replication-dead-letter-limit",
		"REPLICATION_OUTBOX_PATH",
		"-replication-outbox-path",
		"REPLICATION_OUTBOX_CODEC",
		"-replication-outbox-codec",
		"REPLICATION_OUTBOX_BATCH_WINDOW",
		"-replication-outbox-batch-window",
		"REPLICATION_CIRCUIT_BREAKER_FAILURES",
		"-replication-circuit-breaker-failures",
		"REPLICATION_CIRCUIT_BREAKER_COOLDOWN",
		"-replication-circuit-breaker-cooldown",
		"REPLICATION_BATCH_MAX_BYTES",
		"-replication-batch-max-bytes",
		"REPLICATION_TRANSPORT",
		"-replication-transport",
		"REPLICATION_HTTP_FALLBACK",
		"-replication-http-fallback",
		"REPLICATION_MAX_IN_FLIGHT_TARGETS",
		"-replication-max-in-flight-targets",
		"JOURNAL_PULL_FULL_SYNC_FALLBACK",
		"-journal-pull-full-sync-fallback",
		"DB_COMPACT_INTERVAL",
		"-db-compact-interval",
		"DB_COMPACT_START_KEY",
		"-db-compact-start-key",
		"DB_COMPACT_LIMIT_KEY",
		"-db-compact-limit-key",
		"DB_MEMORY_CAP_BYTES",
		"-db-memory-cap-bytes",
		"DB_RSS_CAP_BYTES",
		"-db-rss-cap-bytes",
		"DB_MEMORY_EVICT_INTERVAL",
		"-db-memory-evict-interval",
		"DB_MEMORY_EVICT_MIN_VALUE_BYTES",
		"-db-memory-evict-min-value-bytes",
	} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("monitoring server script missing token %q", token)
		}
	}
	data, err = os.ReadFile("scripts/benchmark-journal-catchup.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/benchmark-journal-catchup.sh) error = %v", err)
	}
	for _, token := range []string{
		"JOURNAL_CATCHUP_BENCH",
		"BENCHTIME",
		"go test",
	} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("journal catch-up benchmark script missing token %q", token)
		}
	}
	data, err = os.ReadFile("scripts/restore-bundle.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/restore-bundle.sh) error = %v", err)
	}
	for _, token := range []string{
		"RESTORE_BUNDLE_PATH",
		"restore-bundle -bundle",
		"-data-dir",
		"-overwrite",
	} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("restore-bundle script missing token %q", token)
		}
	}
	data, err = os.ReadFile("scripts/restore-rehearsal.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/restore-rehearsal.sh) error = %v", err)
	}
	for _, token := range []string{
		"RESTORE_REHEARSAL_PATH",
		"restore-rehearsal -path",
		"RESTORE_REHEARSAL_WORK_DIR",
		"-keep-work-dir",
		"RESTORE_REHEARSAL_RUNTIME_CHECK",
		"-runtime-check",
		"RESTORE_REHEARSAL_RUNTIME_GET",
		"-runtime-get",
		"RESTORE_REHEARSAL_RUNTIME_SERVER_BIN",
		"-runtime-server-bin",
	} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("restore-rehearsal script missing token %q", token)
		}
	}
}

func TestFrontendSmokeScriptIsWiredThroughMakefile(t *testing.T) {
	data, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	makefile := string(data)
	for _, token := range []string{
		"frontend-smoke:",
		"./scripts/frontend-smoke.sh",
		"frontend-backend-smoke:",
		"./scripts/frontend-backend-smoke.sh",
	} {
		if !strings.Contains(makefile, token) {
			t.Fatalf("Makefile missing frontend smoke token %q", token)
		}
	}

	data, err = os.ReadFile("scripts/frontend.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/frontend.sh) error = %v", err)
	}
	frontendScript := string(data)
	for _, token := range []string{
		"smoke)",
		"FRONTEND_SMOKE_SKIP_BUILD=true",
		"frontend-smoke.sh",
	} {
		if !strings.Contains(frontendScript, token) {
			t.Fatalf("frontend.sh missing smoke token %q", token)
		}
	}

	data, err = os.ReadFile("scripts/frontend-smoke.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/frontend-smoke.sh) error = %v", err)
	}
	smokeScript := string(data)
	for _, token := range []string{
		"vite preview",
		"/admin.html",
		"Audit Trail",
		"FRONTEND_SMOKE_REQUIRE_BROWSER",
		"Chrome/Chromium not found",
	} {
		if !strings.Contains(smokeScript, token) {
			t.Fatalf("frontend-smoke.sh missing token %q", token)
		}
	}

	data, err = os.ReadFile("scripts/frontend-backend-smoke.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/frontend-backend-smoke.sh) error = %v", err)
	}
	backendSmokeScript := string(data)
	for _, token := range []string{
		"go build -o",
		"-monitoring-server",
		"-db-path",
		"-audit-log-path",
		"/api/storage/flush",
		"/api/replication",
		"storage.flush",
		"FRONTEND_BACKEND_SMOKE_REQUIRE_BROWSER",
	} {
		if !strings.Contains(backendSmokeScript, token) {
			t.Fatalf("frontend-backend-smoke.sh missing token %q", token)
		}
	}
}

func writeTestFile(t *testing.T, path string, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
}

func runScript(t *testing.T, script string, env ...string) {
	t.Helper()
	output, err := runScriptOutput(script, env...)
	if err != nil {
		t.Fatalf("%s failed: %v\n%s", script, err, output)
	}
}

func runScriptOutput(script string, env ...string) (string, error) {
	cmd := exec.Command("sh", script)
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.CombinedOutput()
	return string(output), err
}
