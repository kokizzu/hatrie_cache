#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU20JSONQueryRowSource' -benchmem -benchtime=200ms -count=5
