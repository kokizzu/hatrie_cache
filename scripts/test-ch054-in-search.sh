#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestCH054In' -count=1
