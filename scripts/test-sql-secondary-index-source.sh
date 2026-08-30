#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLSecondaryIndexesRefreshAfterStringReplacement$' -count=1
