#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^TestIncrementalIntervalJoin' -count=1
