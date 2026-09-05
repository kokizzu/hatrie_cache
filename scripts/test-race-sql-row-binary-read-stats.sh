#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestAnalyzeSQLRowBinaryRead' -count=1
