#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMZ047' -count=1
