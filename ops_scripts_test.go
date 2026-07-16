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
		"doctor:",
		"./scripts/doctor.sh",
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
	} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("restore-rehearsal script missing token %q", token)
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
