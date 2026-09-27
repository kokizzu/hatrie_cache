#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkMZ004DurableJob(Binary|JSON)(Encode|Decode)$' -benchmem -count=5
