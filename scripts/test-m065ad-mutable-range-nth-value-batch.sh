#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^TestMutableIncrementalRangeNthValueWindow' -count=1
