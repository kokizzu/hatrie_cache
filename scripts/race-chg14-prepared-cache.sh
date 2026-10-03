#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestSQLPreparedQueryCache' -count=1
