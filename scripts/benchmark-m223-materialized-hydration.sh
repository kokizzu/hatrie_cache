#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM223MaterializedView(CreateControl|ColdHydrate)$' -benchmem -benchtime=100ms -count=5
