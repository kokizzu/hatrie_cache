#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLSelectStarExcept$' -count=1
