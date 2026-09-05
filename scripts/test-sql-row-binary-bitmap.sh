#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestSQLRowBinaryBitmap|TestEncodeSQLRowBinaryBitmap' -count=1
