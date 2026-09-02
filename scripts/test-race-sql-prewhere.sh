#!/bin/sh
set -eu

go test -race ./hat/hatSql -run '^TestSQLPrewhere' -count=1
