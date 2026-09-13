#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestCH051Regex' -count=1
