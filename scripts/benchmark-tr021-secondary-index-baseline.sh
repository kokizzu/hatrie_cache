#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTT021LegacyPublicScanIndexBuild$' -benchmem -benchtime=1s -count=5
