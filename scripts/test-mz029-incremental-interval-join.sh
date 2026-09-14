#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestIncrementalIntervalJoin' -count=1
