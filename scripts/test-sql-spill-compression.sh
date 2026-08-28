#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestCompressedSQLSortSpill' -count=1
