#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCostedExplainMZ044(Regular|Costed)$' -benchmem -benchtime=100ms -count=5
