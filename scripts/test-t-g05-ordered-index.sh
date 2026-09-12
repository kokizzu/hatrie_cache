#!/usr/bin/env bash
set -euo pipefail

go test -count=1 ./hat/hatDataStructure -run '^TestOrderedIndex'
