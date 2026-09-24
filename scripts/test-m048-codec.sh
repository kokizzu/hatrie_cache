#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM048DataflowPlan' -count=1
