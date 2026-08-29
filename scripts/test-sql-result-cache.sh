#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestResultCache' -count=1
