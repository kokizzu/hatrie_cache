#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCHU24' -count=1
