#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkC240BackupRestoreAndQuery$' -benchtime=1x -count=1
