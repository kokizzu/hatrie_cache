#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkTypedTableStats|BenchmarkTypedTableColumnarSource|BenchmarkSQLColumnarMinMaxMetadata)$' -benchmem -count=5
