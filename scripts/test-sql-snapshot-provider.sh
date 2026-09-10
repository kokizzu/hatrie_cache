#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLSnapshotProvider' -count=1
