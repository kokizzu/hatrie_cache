#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run 'TestObjectStore(Target|Encrypted)' -count=1
test -s BACKUP_ENCRYPTION.md
grep -q 'BACKUP_ENCRYPTION.md' README.md
grep -q '## CH-49: Encrypted Object-Store Backups' BENCHMARK.md
grep -q '| CH-49 | Encrypted backups with key rotation metadata .*\[x\]' INSPIRATION_BACKLOG.md
git diff --check -- BENCHMARK.md BACKUP_ENCRYPTION.md INSPIRATION_BACKLOG.md Makefile README.md hat/hatBackup/chain.go hat/hatBackup/encryption.go hat/hatBackup/encryption_test.go hat/hatBackup/model.go hat/hatBackup/object_store.go scripts
