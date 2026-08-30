#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLTypedInt64Index' -count=1
