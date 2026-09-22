#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH004' -count=1 -timeout=2m
