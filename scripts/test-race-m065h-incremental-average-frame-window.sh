#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestIncrementalFrameWindowAverage' -count=1
