#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatSql -run '^TestCH053In' -count=1
