#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCHU26' -count=1
