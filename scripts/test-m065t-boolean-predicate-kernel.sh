#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLColumnarBooleanPredicateKernel' -count=1
