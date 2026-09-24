#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQLCompiledQueryCacheCoalescesConcurrent(ExactMisses|CompileErrors)$' -count=1
