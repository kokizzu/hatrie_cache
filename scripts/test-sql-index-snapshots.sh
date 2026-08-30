#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONIndexesShareSourceSnapshot$' -count=1
