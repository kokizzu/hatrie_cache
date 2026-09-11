#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM065q' -count=1
