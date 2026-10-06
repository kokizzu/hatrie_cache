#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatBackup -run '^$' -bench 'BenchmarkC240' -benchmem -count=5 -benchtime=200ms
