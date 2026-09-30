#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestAppendSQLResultCacheBytesPartMatchesString|TestSQLResultCacheKey' -count=1
