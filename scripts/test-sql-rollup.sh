#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestTimeBucketRollup' -count=1
