#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestTR022|^TestSQLIndexRebuildVerificationPublicAPI$' -count=1
