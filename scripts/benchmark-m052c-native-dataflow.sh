#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkCompiledSQLDataflow' -benchmem -count=5
