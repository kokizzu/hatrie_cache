#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLPreparedQueryCache' -count=1
