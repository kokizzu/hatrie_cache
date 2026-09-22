#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM220PointLookup' -count=1
