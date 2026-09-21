#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkC241BackupRepositoryWholeFileBaseline$' -benchmem -benchtime=1x -count=5
