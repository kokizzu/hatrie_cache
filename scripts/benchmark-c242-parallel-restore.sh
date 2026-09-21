#!/usr/bin/env bash
set -euo pipefail

benchtime="${BENCHTIME:-1s}"
go test ./hat/hatBackup -run '^$' -bench '^BenchmarkCopyRestoreFiles$' -benchmem -benchtime="$benchtime"
