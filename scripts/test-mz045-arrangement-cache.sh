#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMZ045ArrangementRecommendationCache' -count=1 -timeout=2m
