#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'CH031|CHU20|JSON' -count=1
