#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatHash -run '^TestFNV1a64(Int64|Uint64)' -count=1
