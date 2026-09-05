#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestSQLStringDictionary' -count=1
