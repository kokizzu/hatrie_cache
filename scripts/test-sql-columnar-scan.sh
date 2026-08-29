#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnar' -count=1
