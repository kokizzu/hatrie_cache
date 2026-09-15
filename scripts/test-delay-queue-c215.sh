#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run 'DelayQueue|VisibilityQueue' -count=1
