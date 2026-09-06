#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
gofmt -w hat/hatSql/late_data_policy.go hat/hatSql/late_data_policy_test.go
