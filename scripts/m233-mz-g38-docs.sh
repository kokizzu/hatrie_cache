#!/usr/bin/env bash
set -euo pipefail

test -s MZ038_SOURCE_LAG_ALERTS.md
rg -n '^## MZ038 Source Lag Alerts$' BENCHMARK.md
rg -n 'MZ038_SOURCE_LAG_ALERTS.md' README.md
rg -n '^\| MZ-G38 \|.*Implemented and measured' IDEA_GAP_CATALOG.md
