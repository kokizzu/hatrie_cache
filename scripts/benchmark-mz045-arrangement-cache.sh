#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ045ArrangementRecommendation' -benchmem -count=5 -timeout=2m
