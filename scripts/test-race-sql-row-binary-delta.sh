#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestSQLRowBinaryDelta' -count=1
