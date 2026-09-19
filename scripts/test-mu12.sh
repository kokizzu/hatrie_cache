#!/usr/bin/env bash
set -euo pipefail

go test -tags mu12 ./hat/hatSql -run 'Test(BuildExplainArrangementPlan|ExplainArrangement)' -count=1 "$@"
