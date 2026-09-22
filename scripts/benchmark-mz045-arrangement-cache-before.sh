#!/usr/bin/env bash
set -euo pipefail

go test -tags=mz045baseline ./hat/hatSql -run '^$' -bench '^BenchmarkMZ045ArrangementRecommendationBaseline$' -benchmem -count=5 -timeout=2m
