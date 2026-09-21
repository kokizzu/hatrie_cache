#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCH230' -count=1
