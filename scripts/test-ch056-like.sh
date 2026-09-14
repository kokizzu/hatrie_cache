#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH056' -count=1
