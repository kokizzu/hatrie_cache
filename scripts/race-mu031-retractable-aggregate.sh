#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestMU031' -count=1
