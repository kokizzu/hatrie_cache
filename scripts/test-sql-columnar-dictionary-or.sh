#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarDictionaryLiteralORUsesCodeFilter$' -count=1
