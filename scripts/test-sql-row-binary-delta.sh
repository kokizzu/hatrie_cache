#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestSQLRowBinaryDelta' -count=1
