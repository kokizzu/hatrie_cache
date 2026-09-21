#!/usr/bin/env bash
set -euo pipefail

go test -v ./hat/hatSql -run '^TestSQLLeftArrayJoinPreservesRowsForEmptyArrays$' -count=1
