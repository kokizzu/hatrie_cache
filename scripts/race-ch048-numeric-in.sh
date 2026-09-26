#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCH048NumericIN' -count=1
