#!/usr/bin/env sh
set -eu

go test -race ./hat/hatSql -run '^TestSQL.*Parallel.*Merge' -count=1
