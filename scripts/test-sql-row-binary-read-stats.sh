#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestAnalyzeSQLRowBinaryRead' -count=1
