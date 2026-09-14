#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestMZ040|TestIncrementalPercentile)' -count=1
