#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkPreviewMigration$' -benchmem -benchtime=1s -count=5
