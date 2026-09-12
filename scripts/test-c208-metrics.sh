#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^Test(ResultCacheStats|SQLQueryResultCacheStats)' -count=1
