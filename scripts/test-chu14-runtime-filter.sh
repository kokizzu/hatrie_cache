#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestRuntimeJoinBloomFilter' -count=1 "$@"
