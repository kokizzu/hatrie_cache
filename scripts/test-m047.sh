#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM047' -count=1
