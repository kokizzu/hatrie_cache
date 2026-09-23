#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatSql -run '^TestCH001' -count=1
