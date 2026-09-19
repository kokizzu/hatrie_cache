#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCHU26' -count=1
