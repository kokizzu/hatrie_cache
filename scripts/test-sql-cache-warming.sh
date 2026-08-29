#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestCacheWarmer' -count=1
