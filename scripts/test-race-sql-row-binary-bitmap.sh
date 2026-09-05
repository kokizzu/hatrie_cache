#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestSQLRowBinaryBitmap|TestEncodeSQLRowBinaryBitmap' -count=1
