#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLSpillBloomFilters' -count=1
