#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCompiledSQLOrderedLimit' -benchmem -count=5
