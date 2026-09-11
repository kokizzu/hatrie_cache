#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkRestoreBundleResumeExtraction$' -benchmem -count=5
