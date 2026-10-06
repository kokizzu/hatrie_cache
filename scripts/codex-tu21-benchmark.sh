#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkSpaceMigration' -benchmem -benchtime=200ms -count=5 -cpu=1
