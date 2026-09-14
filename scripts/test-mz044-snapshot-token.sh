#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQLSnapshotToken' -count=1
