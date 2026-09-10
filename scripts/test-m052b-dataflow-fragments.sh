#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^(TestCompileSQLDataflow|TestSQLDataflowExecutor|TestCompiledSQLQueryCompilesExecutableDataflow|ExampleCompiledSQLQuery_CompileDataflow)$' -count=1
