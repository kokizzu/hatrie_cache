#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run 'TestHyperLogLogMerge' -count=1
