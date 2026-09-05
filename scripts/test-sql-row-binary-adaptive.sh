#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestSQLRowBinaryAdaptive' -count=1
