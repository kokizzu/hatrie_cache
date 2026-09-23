#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCHG04RuntimeBloomFilterFalsePositiveRate$' -v -count=1
