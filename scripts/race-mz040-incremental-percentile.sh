#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestMZ040|TestIncrementalPercentile)' -count=1
