#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH004Final($|/|Pushdown(Baseline|FastPath)$)' -benchmem -count=5
