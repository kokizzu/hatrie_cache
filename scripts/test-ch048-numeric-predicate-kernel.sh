#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLColumnarNumericPredicateKernel' -count=1
