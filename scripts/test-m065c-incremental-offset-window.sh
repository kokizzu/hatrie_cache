#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^TestIncrementalOffsetWindow' -count=1
