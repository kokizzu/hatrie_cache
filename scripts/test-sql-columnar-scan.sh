#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarScan' -count=1
