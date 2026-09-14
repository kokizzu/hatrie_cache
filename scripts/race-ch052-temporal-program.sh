#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatSql -run '^TestCH052Temporal' -count=1
