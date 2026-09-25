#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQL(RollupCubeAndGroupingSets|GroupingSetsUsesOnePassPlan)$' -count=1
