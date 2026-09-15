#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestU64PostingListValuesPreallocatesExactResult$' -count=1
