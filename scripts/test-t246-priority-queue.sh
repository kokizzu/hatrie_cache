#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestT246PriorityVisibilityQueue' -count=1
