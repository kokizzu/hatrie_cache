#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestT247DeduplicatingPriorityVisibilityQueue' -count=1
