#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
go test ./hat/hatSql -run '^TestSQLLimitBy' -count=1
