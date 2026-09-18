#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestTR022|^TestSQLIndexRebuildVerificationPublicAPI$' -count=1
