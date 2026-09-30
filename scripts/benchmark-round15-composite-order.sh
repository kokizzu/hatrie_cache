#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkRound15TypedTableColumnarCompositeInt64Order$' -benchmem -benchtime=250ms -count=5
