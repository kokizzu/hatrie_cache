#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'Test(ResultCacheStats|SQLQueryResultCacheStats)' -count=1
