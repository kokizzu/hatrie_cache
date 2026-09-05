#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestCanSkipSQLRowBinaryStats' -count=1
