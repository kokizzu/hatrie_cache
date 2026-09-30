#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkRound14TypedTableColumnarInt64Order$' -benchmem -benchtime=250ms -count=5
