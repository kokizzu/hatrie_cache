#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestIncrementalOffsetWindow' -count=10
