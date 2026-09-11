#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQL(QueryLog|QueryManagerWritesCompletedQueryLog)' -count=1
