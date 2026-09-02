#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLPrewhere' -count=1
