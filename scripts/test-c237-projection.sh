#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestC237Explain' -count=1
