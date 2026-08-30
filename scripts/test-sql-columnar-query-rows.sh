#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestExecuteSQLQueryRowsStreamsColumnarSource$' -count=1
