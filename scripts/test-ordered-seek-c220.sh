#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run 'TestOrderedIndex' -count=1
