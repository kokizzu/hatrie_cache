#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestIncrementalFrameWindowCountDistinct' -count=1
