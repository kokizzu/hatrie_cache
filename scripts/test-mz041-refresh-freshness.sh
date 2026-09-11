#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestManagedRefreshScheduler.*' -count=1
