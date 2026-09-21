#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM208' -count=1
