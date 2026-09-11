#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestSQLOrderedRangePruningUsesRealIndexes$' -count=1
