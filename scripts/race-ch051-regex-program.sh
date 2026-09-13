#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatSql -run '^TestCH051Regex' -count=1
