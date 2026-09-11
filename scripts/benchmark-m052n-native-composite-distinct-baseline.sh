#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCompiledSQLNativeCompositeDistinctBaseline$' -benchmem -count=5
