#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestMZ045ArrangementRecommendationCache' -count=5 -timeout=3m
