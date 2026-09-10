#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql \
  -run '^(TestCompileSQLDataflow|TestSQLDataflowExecutor|TestCompiledSQLQueryCompilesExecutableDataflow|ExampleCompiledSQLQuery_CompileDataflow)$' \
  -count=1
