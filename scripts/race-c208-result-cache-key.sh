#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestAppendSQLResultCacheBytesPartMatchesString|TestSQLResultCacheKey' -count=1
