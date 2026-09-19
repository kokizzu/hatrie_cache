#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCHU24' -count=1
