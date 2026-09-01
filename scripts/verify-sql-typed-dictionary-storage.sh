#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestTypedTableDictionaryEncodedStringsRemainCorrect$' -count=1
go test -race ./hat/hatSql
