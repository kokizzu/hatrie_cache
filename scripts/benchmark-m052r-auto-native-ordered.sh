#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -count=5 -run '^$' -bench '^BenchmarkCompiledSQLAutomaticNativeOrdered' -benchmem
