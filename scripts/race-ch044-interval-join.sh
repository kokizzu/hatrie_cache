#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestIncrementalIntervalJoin' -count=1
