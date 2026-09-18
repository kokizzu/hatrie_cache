#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMU032' -count=1
