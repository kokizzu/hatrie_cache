#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatSql -run '^TestM052ReusableDataflowPlanView' -count=1
