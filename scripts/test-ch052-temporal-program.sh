#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestCH052Temporal' -count=1
