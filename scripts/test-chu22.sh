#!/bin/sh
set -eu
go test ./hat/hatSql -run '^TestTypedTableAppendColumnar' -count=1
