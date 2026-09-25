#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMZ010' -count=1
