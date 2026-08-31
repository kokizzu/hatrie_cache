#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarDictionaryLikeUsesCodeFilter$' -count=1
