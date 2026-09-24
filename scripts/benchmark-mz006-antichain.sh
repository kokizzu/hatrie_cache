#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ006Antichain' -benchmem -count=5
