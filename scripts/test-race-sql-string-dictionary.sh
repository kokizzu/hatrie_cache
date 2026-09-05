#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestSQLStringDictionary' -count=1
