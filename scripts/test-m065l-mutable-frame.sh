#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^TestMutableIncrementalFrameWindow' \
  -count=1
