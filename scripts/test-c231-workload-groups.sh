#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestC231' -count=1
