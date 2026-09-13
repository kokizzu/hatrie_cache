#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH050(NDJSONBaseline|RowBinaryStream)$' -benchmem -count=5
