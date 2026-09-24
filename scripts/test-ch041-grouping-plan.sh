#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH041GroupingBranchClone' -count=1
