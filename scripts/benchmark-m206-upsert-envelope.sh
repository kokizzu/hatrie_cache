#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM206(DebeziumBaseline|UpsertChangefeed)$' -benchmem -benchtime=2s -count=5
