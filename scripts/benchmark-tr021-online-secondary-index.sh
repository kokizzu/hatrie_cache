#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTT021(LegacyPublicScanIndexBuild|OnlineSecondaryIndexBuild)$' -benchmem -benchtime=1s -count=5
