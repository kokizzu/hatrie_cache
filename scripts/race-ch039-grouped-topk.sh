#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCH039GroupedApproxTopK' -count=1
