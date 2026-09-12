#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
gofmt -w hat/hatStorage/space_memory_quota.go hat/hatStorage/space_memory_quota_test.go
