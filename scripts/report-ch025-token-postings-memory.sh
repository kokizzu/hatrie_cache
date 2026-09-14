#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestCH025TokenPostingsIndexMemoryReport$' -count=1 -v
