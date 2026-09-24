#!/usr/bin/env bash
set -euo pipefail

GOTOOLCHAIN=local go test ./hat/hatSql -run '^TestCH041GroupingBranchClone' -race -count=1
