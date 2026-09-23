#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCHG04RuntimeBloomFilter' -count=1
