#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestU64PostingList' -count=1
