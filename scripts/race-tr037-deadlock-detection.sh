#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestTR037|^ExampleSQLRowLockManager_AcquireOwned_deadlockDetection$' -count=1
