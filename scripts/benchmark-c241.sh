#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^$' -bench 'BenchmarkC241' -benchmem -benchtime=100ms -count=5
