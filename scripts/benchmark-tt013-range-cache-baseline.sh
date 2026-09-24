#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -tags tt013baseline -run '^$' -bench 'BenchmarkTT013RangeScanBaseline' -benchmem -count=5
