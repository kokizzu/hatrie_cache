#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkC241IncrementalBackupRepositoryChunkDedup$' -benchmem -count=5
