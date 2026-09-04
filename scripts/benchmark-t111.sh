#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench 'BenchmarkPersistentStorageSizeLimitCheck' -benchmem -count=3
