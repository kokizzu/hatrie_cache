#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestIncrementalFrameWindowExtrema|^TestIncrementalFrameWindowMinMax' -count=1
