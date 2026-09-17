#!/usr/bin/env bash
set -euo pipefail

exec go test ./hat/hatSql -count=1
