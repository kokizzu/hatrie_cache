#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ044SQL' -benchmem -benchtime=300ms -count=5
