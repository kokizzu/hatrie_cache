#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH030JSONPathLiteralEvaluation$' -benchmem -count=5
