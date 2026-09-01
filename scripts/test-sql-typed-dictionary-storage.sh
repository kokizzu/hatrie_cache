#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestTypedTableDictionaryEncodedStringsRemainCorrect$' -count=1
