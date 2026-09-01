#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONPartialIndexRestrictsAndRefreshes$' -count=1
go test -race ./hat/hatCache ./hat/hatSql
