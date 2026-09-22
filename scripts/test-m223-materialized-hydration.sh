#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM223MaterializedView' -count=1
