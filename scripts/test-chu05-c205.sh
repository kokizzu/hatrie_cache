#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLSubqueryResultCache' -count=1
