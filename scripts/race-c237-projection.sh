#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestC237Explain' -count=1
