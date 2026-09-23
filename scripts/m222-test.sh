#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM222' -count=1
