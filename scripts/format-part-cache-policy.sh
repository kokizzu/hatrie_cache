#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
gofmt -w hat/hatStorage/part_cache_policy.go hat/hatStorage/part_cache_policy_test.go
