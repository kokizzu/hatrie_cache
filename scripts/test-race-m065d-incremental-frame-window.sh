#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestIncrementalFrameWindow' -count=1
