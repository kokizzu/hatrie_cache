#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestCompiledSQLNativeDataflowMatchesStringDistinctRows|TestCompiledSQLNativeDataflowStringDistinctChecksContext|TestCompiledSQLNativeDataflowDistinctRejectsUnsupportedRuntimeKey'
