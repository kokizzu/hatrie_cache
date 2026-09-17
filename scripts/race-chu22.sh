#!/bin/sh
set -eu
go test -race ./hat/hatSql -run '^TestTypedTableAppendColumnar' -count=1
