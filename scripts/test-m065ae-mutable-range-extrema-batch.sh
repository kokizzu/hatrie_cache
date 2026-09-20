#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^TestMutableIncrementalRangeWindow' -count=1
