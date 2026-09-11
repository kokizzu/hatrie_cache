#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM065p' -count=1
