#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCompiledSQLNativeTripleGroupedOrdered' -benchmem -count=5
