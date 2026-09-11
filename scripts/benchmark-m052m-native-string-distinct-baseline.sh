#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCompiledSQLNativeStringDistinctBaseline$' -benchmem -count=5
