#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMU013' -count=1
