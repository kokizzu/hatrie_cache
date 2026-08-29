#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestSQL.*Parallel.*Merge' -count=1
