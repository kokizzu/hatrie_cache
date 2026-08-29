#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLIndexValueKey' -count=1
