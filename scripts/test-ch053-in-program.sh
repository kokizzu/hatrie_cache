#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestCH053In' -count=1
