#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH230' -count=1
