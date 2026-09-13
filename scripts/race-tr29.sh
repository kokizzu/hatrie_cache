#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure -run '^TestOrderedIndex(Reverse|Iterator|Snapshot)' -count=1
