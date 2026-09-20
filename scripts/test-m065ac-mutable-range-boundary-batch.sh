#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^TestMutableIncrementalRangeBoundaryWindow' -count=1
