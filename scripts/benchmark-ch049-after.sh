#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^$' -bench '^BenchmarkObjectStoreTarget(Backup|EncryptedBackup|Restore|EncryptedRestore)$' -benchmem -count=5
