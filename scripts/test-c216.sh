#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestC216' -count=1
