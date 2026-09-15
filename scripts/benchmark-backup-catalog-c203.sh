#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^$' -bench 'BenchmarkBackupManifestCatalog' -benchmem -count=5
