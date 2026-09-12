#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestC216' -count=1
