#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestMZ035' -count=1
